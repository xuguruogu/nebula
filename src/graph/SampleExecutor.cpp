/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/SampleExecutor.h"
#include "base/Base.h"
#include "time/WallClock.h"
#include "utils/NebulaKeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "dataman/ResultSchemaProvider.h"
#include <boost/functional/hash.hpp>
#include "GraphFlags.h"

DEFINE_bool(trace_sample, false, "Whether to dump the detail trace log from one sample request");

namespace nebula {
namespace graph {
SampleExecutor::SampleExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "sample") {
    sentence_ = static_cast<SampleSentence *>(sentence);
}

Status SampleExecutor::prepare() {
    return Status::OK();
}

Status SampleExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setOnVariableVariantGet(onVariableVariantGet_);

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
        status = prepareSampleFilter();
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


void SampleExecutor::execute() {
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
    auto spaceId = ectx()->rctx()->session()->space();

    std::vector<storage::cpp2::YieldColumn> yields;
    for (auto* yield : yields_) {
        storage::cpp2::YieldColumn item;
        if(yield->alias()) {
            item.set_alias(*yield->alias());
        } else {
            item.set_alias(yield->expr()->toString());
        }
        item.set_fun_name(yield->getFunName());
        item.set_expr(Expression::encode(yield->expr()));
        yields.push_back(std::move(item));
    }

    auto future  = ectx()->getStorageClient()->sampleNeighbors(spaceId,
                                                              scheme_->toSchema(),
                                                              std::move(starts_),
                                                              vid_idx_,
                                                              edgeTypes_,
                                                              sampleFilter_,
                                                              yields);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto && result) {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get neighbors failed"));
            return;
        } else if (completeness != 100) {
            // TODO(dutor) We ought to let the user know that the execution was partially
            // performed, even in the case that this happened in the intermediate process.
            // Or, make this case configurable at runtime.
            // For now, we just do some logging and keep going.
            LOG(INFO) << "Sample neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            if (!FLAGS_enable_partial_success) {
                doError(Status::Error("Sample neighbors partially failed"));
                return;
            }
            ectx()->addWarningMsg("Sample neighbors was partially performed");
        }
        finishExecution (std::move(result));
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when handle out-bounds/in-bounds: " << e.what();
        doError(Status::Error("Exception when handle out-bounds/in-bounds: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void SampleExecutor::onEmptyInputs() {
    auto cols = result_columns_;
    auto outputs = std::make_unique<InterimResult>(std::move(cols));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}

static Status _getInt(Expression* expr, int64_t &v) {
    Getters getters;

    auto status = expr->prepare();
    if (!status.ok()) {
        return status;
    }
    auto value = expr->eval(getters);
    if (!value.ok()) {
        return status;
    }
    auto r = value.value();
    if (!Expression::isInt(r)) {
        return Status::Error("should be of type integer");
    }
    v = Expression::asInt(r);

    return Status::OK();
}

Status SampleExecutor::prepareSampleFilter() {
    Status status = Status::OK();
    {
        auto *orderBy = sentence_->orderBy();
        if (orderBy == nullptr) {
            LOG(ERROR) << "From clause shall never be null";
            return Status::Error("From clause shall never be null");
        }
        sampleFilter_.set_order_by(Expression::encode(orderBy));
    }

    {
        int64_t limit = 0;
        auto expr = sentence_->limit();
        if (expr) {
            auto stat = _getInt(expr, limit);
            if (!stat.ok()) {
                return stat;
            }

        } else {
            limit= std::numeric_limits<int32_t>::max();
        }
        sampleFilter_.set_limit_size(limit);
    }
    return status;

}


Status SampleExecutor::prepareFrom() {
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
            auto v = value.value();
            if (!Expression::isInt(v)) {
                status = Status::Error("Vertex ID should be of type integer");
                break;
            }
            addStart(Expression::asInt(v));
        }
        fromType_ = kInstantExpr;
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

void SampleExecutor::addStart(VertexID vid) {
    graph::cpp2::RowValue row;
    cpp2::ColumnValue col;
    col.set_id(vid);
    row.set_columns({col});
    starts_.emplace_back(row);
}



Status SampleExecutor::addToEdgeTypes(EdgeType type) {
    switch (direction_) {
        case OverClause::Direction::kForward: {
            edgeTypes_.push_back(type);
            break;
        }
        case OverClause::Direction::kBackward: {
            type = -type;
            edgeTypes_.push_back(type);
            break;
        }
        case OverClause::Direction::kBidirect: {
            edgeTypes_.push_back(type);
            type = -type;
            edgeTypes_.push_back(type);
            break;
        }
        default: {
            return Status::Error("Unkown direction type: %ld", static_cast<int64_t>(direction_));
        }
    }
    return Status::OK();
}

Status SampleExecutor::prepareOverAll() {
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
        auto status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

        if (!expCtx_->addEdge(e, std::abs(v))) {
            return Status::Error(folly::sformat("edge alias({}) was dup", e));
        }
    }

    return Status::OK();
}


Status SampleExecutor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(ERROR) << "Over clause shall never be null";
        return Status::Error("Over clause shall never be null");
    }

    direction_ = clause->direction();

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
        status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

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

Status SampleExecutor::prepareYield() {
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

        for (auto* yield : yields_) {
            if (!yield->getFunName().empty()) {
                return Status::SyntaxError("Do not support in aggregated query without group by");
            }

            if (yield->alias()) {
                result_columns_.emplace_back(*yield->alias());
            } else {
                result_columns_.emplace_back(yield->expr()->toString());
            }

        }

    }
    return Status::OK();
}

Status SampleExecutor::prepareNeededProps() {
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

Status SampleExecutor::checkNeededProps() {
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

Status SampleExecutor::setupStarts() {
    // Literal vertex ids
    if (!starts_.empty()) {
        auto schemaWriter = std::make_shared<SchemaWriter>();
        schemaWriter->appendCol("id", nebula::cpp2::SupportedType::VID);
        scheme_ = std::move(schemaWriter);
        vid_idx_ = 0;
        return Status::OK();
    }
    const InterimResult* p_inputs = inputs_.get();

    // Take one column from a variable
    if (varname_ != nullptr) {
        bool existing = false;
        auto *varInputs = ectx()->variableHolder()->get(*varname_, &existing);
        if (varInputs == nullptr && !existing) {
            return Status::Error("Variable `%s' not defined", varname_->c_str());
        }
        DCHECK(p_inputs == nullptr);
        p_inputs = varInputs;
    }

    // No error happened, but we are having empty inputs
    if (p_inputs == nullptr || !p_inputs->hasData() ) {
        return Status::OK();
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        return status;
    }
    scheme_ = p_inputs->schema();
    vid_idx_ = scheme_->getFieldIndex(*colname_);
    if (vid_idx_ == -1) {
        return Status::Error("Get vid fail: `%s'", colname_->c_str());
    }
    starts_ = std::move(p_inputs->getRows()).value();
    return Status::OK();
}


void SampleExecutor::finishExecution(RpcResponse && rpcResp) {
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
    if (rows.empty()) {
        onEmptyInputs();
        return;
    }
    // on result
    if (onResult_) {
        auto schema = std::make_shared<SchemaWriter>();
        {
            auto& record = rows.front().get_columns();
            if (record.size() != result_columns_.size()) {
                LOG(ERROR) << "Record size: " << record.size()
                           << " != result_columns_ size: " << result_columns_.size();
                return;
            }
            for (auto i = 0u; i < record.size(); i++) {
                nebula::cpp2::SupportedType type(nebula::cpp2::SupportedType::UNKNOWN);
                switch (record[i].getType()) {
                    case nebula::graph::cpp2::ColumnValue::Type::id:
                    case nebula::graph::cpp2::ColumnValue::Type::integer:
                        type = nebula::cpp2::SupportedType::INT;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::bool_val:
                        type = nebula::cpp2::SupportedType::BOOL;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::double_precision:
                    case nebula::graph::cpp2::ColumnValue::Type::single_precision:
                        type = nebula::cpp2::SupportedType::FLOAT;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::str:
                        type = nebula::cpp2::SupportedType::STRING;
                        break;
                    case nebula::graph::cpp2::ColumnValue::Type::timestamp:
                        type = nebula::cpp2::SupportedType::TIMESTAMP;
                        break;
                    default:
                        break;
                }
                schema->appendCol(result_columns_[i], type);
            }
        }
        auto cols = result_columns_;
        auto result = std::make_unique<InterimResult>(std::move(cols));
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
        result->setInterim(std::move(rsWriter));
        onResult_(std::move(result));

    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(result_columns_);
        resp_->set_rows(std::move(rows));
    }
    doFinish(Executor::ProcessControl::kNext);
}

void SampleExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula