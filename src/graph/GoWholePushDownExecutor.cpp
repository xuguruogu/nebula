/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GoWholePushDownExecutor.h"
#include "graph/SchemaHelper.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "dataman/ResultSchemaProvider.h"
#include <boost/functional/hash.hpp>

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

GoWholePushDownExecutor::GoWholePushDownExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "Go_with_order_by_and_limit") {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<GoSentence*>(sentence);
}

Status GoWholePushDownExecutor::prepare() {
    return Status::OK();
}

Status GoWholePushDownExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());

    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }
        status = prepareFrom();
        if (!status.ok()) {
            break;
        }
        status = prepareOver();
        if (!status.ok()) {
            break;
        }
        status = prepareWhere();
        if (!status.ok()) {
            break;
        }
        status = prepareYield();
        if (!status.ok()) {
            break;
        }
        status = prepareNeededProps();
        if (!status.ok()) {
            break;
        }
        status = prepareDistinct();
        if (!status.ok()) {
            break;
        }
        status = prepareLimit();
        if (!status.ok()) {
            break;
        }
        status = prepareOrderBy();
        if (!status.ok()) {
            break;
        }
        status = checkNeededProps();
        if (!status.ok()) {
            break;
        }
    } while (false);

    if (!status.ok()) {
        LOG(ERROR) << "Preparing failed: " << status;
        return status;
    }

    return status;
}

void GoWholePushDownExecutor::execute() {
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    status = setupStarts();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    if (starts_.empty()) {
        onEmptyInputs();
        return;
    }
    if (distinct_) {
        std::unordered_set<VertexID> uniqID;
        for (auto id : starts_) {
            uniqID.emplace(id);
        }
        starts_ = std::vector<VertexID>(uniqID.begin(), uniqID.end());
    }
    stepOut();
}

Status GoWholePushDownExecutor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
    do {
        if (clause == nullptr) {
            LOG(ERROR) << "From clause shall never be null";
            return Status::Error("From clause shall never be null");
        }

        if (clause->isRef()) {
            auto *expr = clause->ref();
            if (expr->isInputExpression()) {
                fromType_ = kPipe;
                auto *iexpr = static_cast<InputPropertyExpression*>(expr);
                colname_ = iexpr->prop();
            } else if (expr->isVariableExpression()) {
                fromType_ = kVariable;
                auto *vexpr = static_cast<VariablePropertyExpression*>(expr);
                varname_ = vexpr->alias();
                colname_ = vexpr->prop();
            } else {
                // No way to happen except memory corruption
                LOG(ERROR) << "Unknown kind of expression";
                return Status::Error("Unknown kind of expression");
            }

            if (colname_ != nullptr && *colname_ == "*") {
                status = Status::Error("Can not use `*' to reference a vertex id column.");
                break;
            }
            break;
        }

        auto space = ectx()->rctx()->session()->space();
        expCtx_->setSpace(space);
        auto vidList = clause->vidList();
        Getters getters;
        for (auto *expr : vidList) {
            expr->setContext(expCtx_.get());

            status = expr->prepare();
            if (!status.ok()) {
                break;
            }
            auto value = expr->eval(getters);
            if (!value.ok()) {
                status = Status::Error();
                break;
            }
            if (expr->isFunCallExpression()) {
                auto *funcExpr = static_cast<FunctionCallExpression*>(expr);
                if (*(funcExpr->name()) == "near") {
                    auto v = Expression::asString(value.value());
                    std::vector<VertexID> result;
                    folly::split(",", v, result, true);
                    starts_.insert(starts_.end(),
                                   std::make_move_iterator(result.begin()),
                                   std::make_move_iterator(result.end()));
                    continue;
                }
            }
            auto v = value.value();
            if (!Expression::isInt(v)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            starts_.push_back(Expression::asInt(v));
        }
        fromType_ = kInstantExpr;
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status GoWholePushDownExecutor::prepareOverAll() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto edgeAllStatus = ectx()->schemaManager()->getAllEdge(spaceId);

    if (!edgeAllStatus.ok()) {
        return edgeAllStatus.status();
    }

    auto allEdge = edgeAllStatus.value();
    for (auto &e : allEdge) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, e);
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        edgeTypes_.push_back(v);

        if (!expCtx_->addEdge(e, std::abs(v))) {
            return Status::Error(folly::sformat("edge alias({}) was dup", e));
        }
    }

    return Status::OK();
}

Status GoWholePushDownExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(ERROR) << "Over clause shall never be null";
        return Status::Error("Over clause shall never be null");
    }

    if (clause->direction() != OverClause::Direction::kForward) {
        LOG(ERROR) << "GoWholePushDownExecutor Over clause shall always be kForward";
        return Status::Error("GoWholePushDownExecutor Over clause shall always be kForward");
    }

    auto edges = clause->edges();
    for (auto e : edges) {
        if (e->isOverAll()) {
            expCtx_->setOverAllEdge();
            return prepareOverAll();
        }

        auto spaceId = ectx()->rctx()->session()->space();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *e->edge());
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        edgeTypes_.push_back(v);

        if (e->alias() != nullptr) {
            if (!expCtx_->addEdge(*e->alias(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->alias()));
            }
        } else {
            if (!expCtx_->addEdge(*e->edge(), std::abs(v))) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->edge()));
            }
        }
    }

    return status;
}

Status GoWholePushDownExecutor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    whereWrapper_ = std::make_unique<WhereWholePushDownWrapper>(clause);
    auto status = whereWrapper_->prepare(expCtx_.get());
    return status;
}

Status GoWholePushDownExecutor::prepareYield() {
    auto *clause = sentence_->yieldClause();
    // this preparation depends on interim result,
    // it can only be called after getting results of the previous executor,
    // but if we can do the semantic analysis before execution,
    // then we can do the preparation before execution
    // TODO: make it possible that this preparation not depends on interim result
    if (clause != nullptr) {
        yieldClauseWrapper_ = std::make_unique<YieldClauseWrapper>(clause);
        auto *varHolder = ectx()->variableHolder();
        auto status = yieldClauseWrapper_->prepare(inputs_.get(), varHolder, yields_);
        if (!status.ok()) {
            return status;
        }
        for (auto *col : yields_) {
            if (!col->getFunName().empty()) {
                return Status::SyntaxError("Do not support in aggregated query without group by");
            }
        }
    }
    return Status::OK();
}


Status GoWholePushDownExecutor::prepareNeededProps() {
    auto status = Status::OK();
    do {
        for (auto *col : yields_) {
            col->expr()->setContext(expCtx_.get());
            status = col->expr()->prepare();
            if (!status.ok()) {
                break;
            }
        }
        if (!status.ok()) {
            break;
        }

        if (expCtx_->hasVariableProp()) {
            if (fromType_ != kVariable) {
                status = Status::Error("A variable must be referred in FROM "
                                       "before used in WHERE or YIELD");
                break;
            }
            auto &variables = expCtx_->variables();
            if (variables.size() > 1) {
                status = Status::Error("Only one variable allowed to use");
                break;
            }
            auto &var = *variables.begin();
            if (var != *varname_) {
                status = Status::Error("Variable name not match: `%s' vs. `%s'",
                                       var.c_str(), varname_->c_str());
                break;
            }
        }

        if (expCtx_->hasInputProp()) {
            if (fromType_ != kPipe) {
                status = Status::Error("`$-' must be referred in FROM "
                                       "before used in WHERE or YIELD");
                break;
            }
        }

        auto &tagMap = expCtx_->getTagMap();
        auto spaceId = ectx()->rctx()->session()->space();
        for (auto &entry : tagMap) {
            auto tagId = ectx()->schemaManager()->toTagID(spaceId, entry.first);
            if (!tagId.ok()) {
                status = Status::Error("Tag `%s' not found.", entry.first.c_str());
                break;
            }
            entry.second = tagId.value();
        }
    } while (false);

    return status;
}

Status GoWholePushDownExecutor::prepareDistinct() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        distinct_ = clause->isDistinct();
        // TODO Consider distinct pushdown later, depends on filter and some other clause pushdown.
        distinctPushDown_ =
            !((expCtx_->hasSrcTagProp() || expCtx_->hasEdgeProp()) && expCtx_->hasDstTagProp());
    }
    return Status::OK();
}

Status GoWholePushDownExecutor::checkNeededProps() {
    auto space = ectx()->rctx()->session()->space();
    TagID tagId;
    auto srcTagProps = expCtx_->srcTagProps();
    auto dstTagProps = expCtx_->dstTagProps();
    srcTagProps.insert(srcTagProps.begin(),
                       std::make_move_iterator(dstTagProps.begin()),
                       std::make_move_iterator(dstTagProps.end()));
    for (auto &pair : srcTagProps) {
        auto found = expCtx_->getTagId(pair.first, tagId);
        if (!found) {
            return Status::Error("Tag `%s' not found.", pair.first.c_str());
        }
        auto ts = ectx()->schemaManager()->getTagSchema(space, tagId);
        if (ts == nullptr) {
            return Status::Error("No tag schema for %s", pair.first.c_str());
        }
        if (ts->getFieldIndex(pair.second) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                                 pair.second.c_str(), pair.first.c_str());
        }
    }

    EdgeType edgeType;
    auto edgeProps = expCtx_->aliasProps();
    for (auto &pair : edgeProps) {
        auto found = expCtx_->getEdgeType(pair.first, edgeType);
        if (!found) {
            return Status::Error("Edge `%s' not found.", pair.first.c_str());
        }
        auto propName = pair.second;
        if (propName == _SRC || propName == _DST
            || propName == _RANK || propName == _TYPE) {
            continue;
        }
        auto es = ectx()->schemaManager()->getEdgeSchema(space, std::abs(edgeType));
        if (es == nullptr) {
            return Status::Error("No edge schema for %s", pair.first.c_str());
        }
        if (es->getFieldIndex(propName) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                                 propName.c_str(), pair.first.c_str());
        }
    }

    return Status::OK();
}

Status GoWholePushDownExecutor::prepareOrderBy() {
    auto layerOrderByClause = sentence_->layerOrderByClause();

    auto factors = layerOrderByClause->factors();
    layerOrderBySortFactors_.reserve(factors.size());
    for (auto &factor : factors) {
        auto expr = static_cast<InputPropertyExpression*>(factor->expr());
        if (factor->orderType() != OrderFactor::OrderType::ASCEND &&
            factor->orderType() != OrderFactor::OrderType::DESCEND) {
            LOG(ERROR) << "Unkown Order Type: " << factor->orderType();
            return Status::Error("Unkown Order Type: %d", factor->orderType());
        }
        auto& prop = *(expr->prop());


        auto find = std::find_if(yields_.begin(), yields_.end(), [&prop] (const YieldColumn* yield) {
            return prop == *yield->alias();
        });
        if (find == yields_.end()) {
            return Status::Error("order by `%s' not found.", prop.c_str());
        }

        nebula::storage::cpp2::OrderByFactor f;
        f.set_idx(std::distance(yields_.begin(), find));
        f.set_type(factor->orderType() == OrderFactor::OrderType::ASCEND ? nebula::storage::cpp2::OrderByType::ASCEND : nebula::storage::cpp2::OrderByType::DESCEND);
        layerOrderBySortFactors_.emplace_back(std::move(f));
    }
    return Status::OK();
}

Status GoWholePushDownExecutor::prepareLimit() {
    auto layerLimitClause = sentence_->layerLimitClause();

    auto offset = layerLimitClause->offset();
    if (offset < 0) {
        return Status::SyntaxError("skip `%ld' is illegal", offset);
    }
    auto count = layerLimitClause->count();
    if (count < 0) {
        return Status::SyntaxError("count `%ld' is illegal", count);
    }

    layerLimitCountPushDown_ = offset + count;
    layerLimitCount_ = count;
    layerLimitOffset_ = offset;
    return Status::OK();
}

Status GoWholePushDownExecutor::setupStarts() {
    // Literal vertex ids
    if (!starts_.empty()) {
        return Status::OK();
    }
    p_inputs_ = inputs_.get();
    // Take one column from a variable
    if (varname_ != nullptr) {
        bool existing = false;
        auto *varInputs = ectx()->variableHolder()->get(*varname_, &existing);
        if (varInputs == nullptr && !existing) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
        DCHECK(p_inputs_ == nullptr);
        p_inputs_ = varInputs;
    }
    // No error happened, but we are having empty inputs
    if (p_inputs_ == nullptr || !p_inputs_->hasData()) {
        return Status::OK();
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        return status;
    }
    auto result = p_inputs_->getVIDs(*colname_);
    if (!result.ok()) {
        LOG(ERROR) << "Get vid fail: " << *colname_;
        return std::move(result).status();
    }
    starts_ = std::move(result).value();

    auto indexResult = p_inputs_->buildIndex(*colname_);
    if (!indexResult.ok()) {
        return std::move(indexResult).status();
    }
    index_ = std::move(indexResult).value();
    return Status::OK();
}

void GoWholePushDownExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void GoWholePushDownExecutor::stepOut() {
    auto spaceId = ectx()->rctx()->session()->space();

    std::vector<storage::cpp2::YieldColumn> yields;
    for (auto yield : yields_) {
        storage::cpp2::YieldColumn item;
        item.set_alias(*yield->alias());
        item.set_fun_name(yield->getFunName());
        item.set_expr(Expression::encode(yield->expr()));
        yields.push_back(std::move(item));
    }

    auto future  = ectx()->getStorageClient()->getNeighborsWholePushDown(
        spaceId,
        p_inputs_->schema()->toSchema(),
        std::move(p_inputs_->getRows()).value(),
        p_inputs_->schema()->getFieldIndex(*colname_),
        edgeTypes_,
        whereWrapper_->getFilterPushdown(),
        yields,
        layerOrderBySortFactors_,
        layerLimitCountPushDown_);

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get neighbors failed"));
            return;
        } else if (completeness != 100) {
            // TODO(dutor) We ought to let the user know that the execution was partially
            // performed, even in the case that this happened in the intermediate process.
            // Or, make this case configurable at runtime.
            // For now, we just do some logging and keep going.
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (FLAGS_trace_go) {
            LOG(INFO) << "GoWholePushDownExecutor finished, total request vertices " << starts_.size();
            auto& hostLatency = result.hostLatency();
            for (size_t i = 0; i < hostLatency.size(); i++) {
                LOG(INFO) << std::get<0>(hostLatency[i])
                          << ", time cost " << std::get<1>(hostLatency[i])
                          << "us / " << std::get<2>(hostLatency[i])
                          << "us, total results " << result.responses()[i].get_rows().size();
            }
        }

        onStepOutResponse(std::move(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle out-bounds/in-bounds: " << e.what();
        doError(Status::Error("Exeception when handle out-bounds/in-bounds: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void GoWholePushDownExecutor::onStepOutResponse(RpcResponse &&rpcResp) {
    auto& all = rpcResp.responses();

    std::vector<cpp2::RowValue> rows;
    uint64_t result_cnt = 0;
    // collcet rows
    {
        for (auto& resp : all) {
            result_cnt += resp.get_rows().size();
        }
        rows.reserve(result_cnt);
        for (auto& resp : all) {
            rows.insert(rows.end(), std::make_move_iterator(resp.get_rows().begin()), std::make_move_iterator(resp.get_rows().end()));
        }
    }
    if (!result_cnt) {
        onEmptyInputs();
        return;
    }

    // layer sort && limit
    if (!layerOrderBySortFactors_.empty()) {
        auto comparator = [this] (cpp2::RowValue& lhs, cpp2::RowValue& rhs) {
            const auto &lhsColumns = lhs.get_columns();
            const auto &rhsColumns = rhs.get_columns();
            for (auto &factor : layerOrderBySortFactors_) {
                auto fieldIndex = factor.get_idx();
                auto orderType = factor.get_type();
                if (fieldIndex < 0 ||
                    fieldIndex >= static_cast<int64_t>(lhsColumns.size()) ||
                    fieldIndex >= static_cast<int64_t>(rhsColumns.size())) {
                    continue;
                }

                if (lhsColumns[fieldIndex] == rhsColumns[fieldIndex]) {
                    continue;
                }
                if (orderType == ::nebula::storage::cpp2::OrderByType::ASCEND) {
                    return lhsColumns[fieldIndex] < rhsColumns[fieldIndex];
                } else if (orderType == ::nebula::storage::cpp2::OrderByType::DESCEND) {
                    return lhsColumns[fieldIndex] > rhsColumns[fieldIndex];
                }
            }
            return false;
        };

        if (layerLimitOffset_) {
            if (rows.size() <= static_cast<uint64_t>(layerLimitOffset_)) {
                rows.clear();
            } else if (rows.size() <= static_cast<uint64_t>(layerLimitOffset_ + layerLimitCount_)) {
                std::nth_element(rows.begin(), rows.begin() + layerLimitOffset_, rows.end(), comparator);
                std::copy(std::make_move_iterator(rows.begin() + layerLimitOffset_),
                          std::make_move_iterator(rows.end()),
                          std::make_move_iterator(rows.begin()));
                rows.resize(rows.size() - layerLimitOffset_);
            } else {
                std::nth_element(rows.begin(), rows.begin() + layerLimitOffset_, rows.end(), comparator);
                std::nth_element(rows.begin() + layerLimitOffset_, rows.begin() + layerLimitOffset_ + layerLimitCount_, rows.end(), comparator);
                std::copy(std::make_move_iterator(rows.begin() + layerLimitOffset_),
                          std::make_move_iterator(rows.begin() + layerLimitOffset_ + layerLimitCount_),
                          std::make_move_iterator(rows.begin()));
                rows.resize(layerLimitCount_);
            }
        } else {
            if (layerLimitCount_ > 0 && rows.size() > static_cast<uint64_t>(layerLimitCount_)) {
                std::nth_element(rows.begin(), rows.begin() + layerLimitCount_, rows.end(), comparator);
                rows.resize(layerLimitCount_);
            }
        }

        std::sort(rows.begin(), rows.end(), comparator);
    }

    if (rows.empty()) {
        onEmptyInputs();
        return;
    }

    // on result
    if (onResult_) {
        auto schema = std::make_shared<SchemaWriter>();
        auto colnames = getResultColumnNames();
        {
            auto& record = rows.front().get_columns();
            if (record.size() != colnames.size()) {
                LOG(ERROR) << "Record size: " << record.size()
                           << " != colnames size: " << colnames.size();
                return;
            }

            for (auto i = 0u; i < record.size(); i++) {
                SupportedType type;
                switch (record[i].getType()) {
                    case nebula::graph::cpp2::ColumnValue::Type::id:
                    case nebula::graph::cpp2::ColumnValue::Type::integer:
                        type = SupportedType::INT;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::bool_val:
                        type = SupportedType::BOOL;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::double_precision:
                    case nebula::graph::cpp2::ColumnValue::Type::single_precision:
                        type = SupportedType::FLOAT;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::str:
                        type = SupportedType::STRING;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::timestamp:
                        type = SupportedType::TIMESTAMP;
                        break;
                    default:
                        break;
                }
                schema->appendCol(colnames[i], type);
            }

        }

        auto result = std::make_unique<InterimResult>(getResultColumnNames());
        auto rsWriter = std::make_unique<RowSetWriter>(schema);
        for (auto& row : rows) {
            RowWriter writer(schema);
            for (auto &column : row.get_columns()) {
                switch (column.getType()) {
                    case cpp2::ColumnValue::Type::id:
                        writer << column.get_id();
                        break;
                    case cpp2::ColumnValue::Type::integer:
                        writer << column.get_integer();
                        break;
                    case cpp2::ColumnValue::Type::double_precision:
                        writer << column.get_double_precision();
                        break;
                    case cpp2::ColumnValue::Type::bool_val:
                        writer << column.get_bool_val();
                        break;
                    case cpp2::ColumnValue::Type::str:
                        writer << column.get_str();
                        break;
                    case cpp2::ColumnValue::Type::timestamp:
                        writer << column.get_timestamp();
                        break;
                    default:
                        LOG(ERROR) << "Not Support type: " << column.getType();
                        break;
                }
            }
            rsWriter->addRow(writer.encode());
        }
        onResult_(std::move(result));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(getResultColumnNames());
        resp_->set_rows(std::move(rows));
    }

    doFinish(Executor::ProcessControl::kNext);
}

std::vector<std::string> GoWholePushDownExecutor::getResultColumnNames() const {
    std::vector<std::string> result;
    result.reserve(yields_.size());
    for (auto *col : yields_) {
        if (col->alias() == nullptr) {
            result.emplace_back(col->expr()->toString());
        } else {
            result.emplace_back(*col->alias());
        }
    }
    return result;
}

void GoWholePushDownExecutor::onEmptyInputs() {
    auto resultColNames = getResultColumnNames();
    auto outputs = std::make_unique<InterimResult>(std::move(resultColNames));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}

}   // namespace graph
}   // namespace nebula
