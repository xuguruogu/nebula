/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GoWholePushDownV2Executor.h"
#include "graph/SchemaHelper.h"
#include "graph/AggregateFunction.h"
#include "dataman/RowReader.h"
#include "dataman/RowSetReader.h"
#include "dataman/ResultSchemaProvider.h"
#include <boost/functional/hash.hpp>

namespace nebula {
namespace graph {

using SchemaProps = std::unordered_map<std::string, std::vector<std::string>>;
using nebula::cpp2::SupportedType;

GoWholePushDownV2Executor::GoWholePushDownV2Executor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx, "Go_with_order_by_and_limit") {
    // The RTTI is guaranteed by Sentence::Kind,
    // so we use `static_cast' instead of `dynamic_cast' for the sake of efficiency.
    sentence_ = static_cast<GoSentence*>(sentence);
}

Status GoWholePushDownV2Executor::prepare() {
    return Status::OK();
}

Status GoWholePushDownV2Executor::prepareClauses() {
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
        status = prepareSubSentences();
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

void GoWholePushDownV2Executor::execute() {
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
        std::vector<graph::cpp2::RowValue> new_starts;
        new_starts.reserve(starts_.size());

        for (auto row : starts_) {
            auto col = row.get_columns()[vid_idx_];
            VertexID vid;
            if (col.getType() == cpp2::ColumnValue::Type::id) {
                vid = col.get_id();
            } else {
                vid = col.get_integer();
            }
            if (uniqID.find(vid) == uniqID.end()) {
                uniqID.emplace(vid);
                new_starts.emplace_back(std::move(row));
            }
        }
        starts_ = std::move(new_starts);
    }

    stepOut();
}

Status GoWholePushDownV2Executor::prepareFrom() {
    Status status = Status::OK();
    auto *clause = sentence_->fromClause();
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
            return Status::Error("Can not use `*' to reference a vertex id column.");
        }
        return status;
    }

    auto space = ectx()->rctx()->session()->space();
    expCtx_->setSpace(space);
    auto vidList = clause->vidList();
    Getters getters;
    for (auto *expr : vidList) {
        expr->setContext(expCtx_.get());
        status = expr->prepare();
        if (!status.ok()) {
            return Status::Error();
        }
        auto value = expr->eval(getters);
        if (!value.ok()) {
            return Status::Error();
        }
        if (expr->isFunCallExpression()) {
            auto *funcExpr = static_cast<FunctionCallExpression*>(expr);
            if (*(funcExpr->name()) == "near") {
                auto v = Expression::asString(value.value());
                std::vector<VertexID> result;
                folly::split(",", v, result, true);
                for (auto vid : result) {
                    addStart(vid);
                }
                continue;
            }
        }
        auto v = value.value();
        if (!Expression::isInt(v)) {
            return Status::Error("Vertex ID should be of type integer");
        }
        addStart(Expression::asInt(v));
    }
    fromType_ = kInstantExpr;
    return status;
}

void GoWholePushDownV2Executor::addStart(VertexID vid) {
    graph::cpp2::RowValue row;
    cpp2::ColumnValue col;
    col.set_id(vid);
    row.set_columns({col});
    starts_.emplace_back(row);
}

void GoWholePushDownV2Executor::addStart(graph::cpp2::RowValue row) {
    starts_.emplace_back(std::move(row));
}

Status GoWholePushDownV2Executor::prepareOverAll() {
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

Status GoWholePushDownV2Executor::prepareOver() {
    Status status = Status::OK();
    auto *clause = sentence_->overClause();
    if (clause == nullptr) {
        LOG(ERROR) << "Over clause shall never be null";
        return Status::Error("Over clause shall never be null");
    }

    if (clause->direction() != OverClause::Direction::kForward) {
        LOG(ERROR) << "GoWholePushDownV2Executor Over clause shall always be kForward";
        return Status::Error("GoWholePushDownV2Executor Over clause shall always be kForward");
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

Status GoWholePushDownV2Executor::prepareWhere() {
    auto *clause = sentence_->whereClause();
    if (clause) {
        clause->filter()->setContext(expCtx_.get());
        auto status = clause->filter()->prepare();
        if (!status.ok()) {
            LOG(ERROR) << status.toString();
            return status;
        }
        if (!clause->filter()->canWholePushDown()) {
            return Status::Error("pushdown go where can not whole pushdown. must be a bug.");
        }
    }
    return Status::OK();
}

Status GoWholePushDownV2Executor::prepareYield() {
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
            if (col->alias()) {
                result_columns_.emplace_back(*col->alias());
            } else {
                result_columns_.emplace_back(col->expr()->toString());
            }
        }
    }
    return Status::OK();
}

Status GoWholePushDownV2Executor::prepareNeededProps() {
    for (auto *col : yields_) {
        col->expr()->setContext(expCtx_.get());
        auto status = col->expr()->prepare();
        if (!status.ok()) {
            return status;
        }
    }

    if (expCtx_->hasVariableProp()) {
        if (fromType_ != kVariable) {
            return Status::Error("A variable must be referred in FROM "
                                   "before used in WHERE or YIELD");
        }
        auto &variables = expCtx_->variables();
        if (variables.size() > 1) {
            return Status::Error("Only one variable allowed to use");
        }
        auto &var = *variables.begin();
        if (var != *varname_) {
            return Status::Error("Variable name not match: `%s' vs. `%s'",
                                   var.c_str(), varname_->c_str());
        }
    }

    if (expCtx_->hasInputProp()) {
        if (fromType_ != kPipe) {
            return Status::Error("`$-' must be referred in FROM "
                                   "before used in WHERE or YIELD");
        }
    }

    auto &tagMap = expCtx_->getTagMap();
    auto spaceId = ectx()->rctx()->session()->space();
    for (auto &entry : tagMap) {
        auto tagId = ectx()->schemaManager()->toTagID(spaceId, entry.first);
        if (!tagId.ok()) {
            return Status::Error("Tag `%s' not found.", entry.first.c_str());
        }
        entry.second = tagId.value();
    }
    return Status::OK();
}

Status GoWholePushDownV2Executor::prepareDistinct() {
    auto *clause = sentence_->yieldClause();
    if (clause != nullptr) {
        distinct_ = clause->isDistinct();
    }
    return Status::OK();
}

Status GoWholePushDownV2Executor::checkNeededProps() {
    auto space = ectx()->rctx()->session()->space();
    TagID tagId;
    if (expCtx_->hasDstTagProp()) {
        return Status::Error("push down, should not use dst tags.");
    }

    auto srcTagProps = expCtx_->srcTagProps();
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

Status GoWholePushDownV2Executor::prepareSubSentences() {
    std::vector<const Sentence*> sentences = sentence_->subSentenses();
    {
        // go sentence.
        nebula::storage::cpp2::GoSentence s;
        s.set_edge_types(edgeTypes_);
        auto where = sentence_->whereClause();
        if (where) s.set_where(Expression::encode(where->filter()));
        std::vector<storage::cpp2::YieldColumn> yields;
        for (auto yield : yields_) {
            storage::cpp2::YieldColumn item;
            item.set_alias(*yield->alias());
            item.set_fun_name(yield->getFunName());
            item.set_expr(Expression::encode(yield->expr()));
            yields.push_back(std::move(item));
        }
        s.set_yields(std::move(yields));
        s.set_distinct(distinct_);
        nebula::storage::cpp2::Sentence ss;
        ss.set_go_sentence(std::move(s));
        sentencesPushDown_.emplace_back(std::move(ss));
    }

    for (auto& sentence : sentences) {
        switch (sentence->kind()) {
            case Sentence::Kind::kOrderBy:
            {
                auto order_by_sentence = static_cast<const OrderBySentence*>(sentence);
                auto factors = order_by_sentence->factors();
                nebula::storage::cpp2::OrderBySentence s;

                for (auto &factor : factors) {
                    if (!factor->expr()->isInputExpression()) {
                        return Status::Error("Order should use input expression. %s", factor->expr()->toString().c_str());
                    }

                    auto expr = static_cast<InputPropertyExpression*>(factor->expr());

                    if (factor->orderType() != OrderFactor::OrderType::ASCEND &&
                        factor->orderType() != OrderFactor::OrderType::DESCEND) {
                        LOG(ERROR) << "Unkown Order Type: " << factor->orderType();
                        return Status::Error("Unkown Order Type: %d", factor->orderType());
                    }
                    auto& prop = *(expr->prop());

                    auto find = std::find(result_columns_.begin(), result_columns_.end(), prop);
                    if (find == result_columns_.end()) {
                        return Status::Error("order by `%s' not found.", prop.c_str());
                    }

                    nebula::storage::cpp2::OrderByFactor f;
                    f.set_idx(std::distance(result_columns_.begin(), find));
                    f.set_type(
                        factor->orderType() == OrderFactor::OrderType::ASCEND ?
                        nebula::storage::cpp2::OrderByType::ASCEND :
                        nebula::storage::cpp2::OrderByType::DESCEND
                    );
                    s.order_by.emplace_back(std::move(f));
                }
                nebula::storage::cpp2::Sentence ss;
                ss.set_order_by_sentence(std::move(s));
                sentencesPushDown_.emplace_back(ss);
            }
                break;
            case Sentence::Kind::kLimit:
            {
                auto limit_sentence = static_cast<const LimitSentence*>(sentence);
                auto offset = limit_sentence->offset();
                if (offset < 0) {
                    return Status::SyntaxError("skip `%ld' is illegal", offset);
                }
                auto count = limit_sentence->count();
                if (count < 0) {
                    return Status::SyntaxError("count `%ld' is illegal", count);
                }
                storage::cpp2::LimitSentence s;
                s.set_offset(offset);
                s.set_count(count);
                nebula::storage::cpp2::Sentence ss;
                ss.set_limit_sentence(std::move(s));
                sentencesPushDown_.emplace_back(ss);
            }
                break;
            case Sentence::Kind::KGroupBy:
            {
                auto group_by_sentence = static_cast<const GroupBySentence*>(sentence);
                auto yields_columns = group_by_sentence->yieldClause()->columns();
                auto group_by_columns = group_by_sentence->groupClause()->columns();

                storage::cpp2::GroupBySentence s;

                std::vector<std::string> new_columns;
                {
                    // columns yield check
                    if (yields_columns.empty()) {
                        return Status::SyntaxError("Yield cols of group by is empty");
                    }
                    for (auto *col : yields_columns) {
                        if ((col->getFunName() != kCount && col->getFunName() != kCountDist)
                            && col->expr()->toString() == "*") {
                            return Status::SyntaxError("Syntax error: near `*'");
                        }

                        if (!col->expr()->isInputExpression()) {
                            return Status::SyntaxError("Group by Yield `%s' should use input fields", col->expr()->toString().c_str());
                        }

                        if (col->alias()) {
                            new_columns.emplace_back(*col->alias());
                        } else {
                            new_columns.emplace_back(col->expr()->toString());
                        }

                        // with alias / function name
                        storage::cpp2::YieldColumn y;
                        y.set_fun_name(col->getFunName());
                        if (col->alias()) {
                            y.set_alias(*col->alias());
                        } else {
                            y.set_alias(col->expr()->toString());
                        }
                        y.set_expr(Expression::encode(col->expr()));
                        s.yields.emplace_back(std::move(y));
                    }
                }
                {
                    //group by check
                    if (group_by_columns.empty()) {
                        return Status::SyntaxError("Group cols is empty");
                    }
                    for (auto *col : group_by_columns) {
                        if (!col->getFunName().empty()) {
                            return Status::SyntaxError("Use invalid group function `%s'", col->getFunName().c_str());
                        }

                        if (!col->expr()->isInputExpression()) {
                            return Status::SyntaxError("Group `%s' should use input fields", col->expr()->toString().c_str());
                        }

                        auto groupName = *static_cast<InputPropertyExpression*>(col->expr())->prop();
                        auto find = std::find(result_columns_.begin(), result_columns_.end(), groupName);
                        if (find == result_columns_.end()) {
                            return Status::Error("Group by `%s' not found.", groupName.c_str());
                        }

                        // no alias / function name
                        storage::cpp2::YieldColumn g;
                        g.set_expr(Expression::encode(col->expr()));
                        s.group.emplace_back(std::move(g));
                    }
                }
                result_columns_ = std::move(new_columns);
                nebula::storage::cpp2::Sentence ss;
                ss.set_group_by_sentence(std::move(s));
                sentencesPushDown_.emplace_back(std::move(ss));
            }
                break;
            case Sentence::Kind::kYield:
            {
                auto yield_sentence = static_cast<const YieldSentence*>(sentence);
                auto where = yield_sentence->where();
                auto yields_columns = yield_sentence->columns();

                storage::cpp2::YieldSentence s;

                std::vector<std::string> new_columns;
                {
                    // where
                    auto expCtx = std::make_unique<ExpressionContext>();
                    if (where) {
                        auto filter = where->filter();
                        filter->setContext(expCtx.get());
                        auto status = filter->prepare();
                        if (!status.ok()) {
                            LOG(ERROR) << status.toString();
                            return status;
                        }
                        if (expCtx->hasVariableProp() || expCtx->hasEdgeProp() || expCtx->hasSrcTagProp() || expCtx->hasDstTagProp()) {
                            return Status::SyntaxError("piped yield filter `%s' should use variable fields", filter->toString().c_str());
                        }
                        s.set_where(Expression::encode(filter));
                    }

                    {
                        bool hasAggFun = false;
                        for (auto *col : yields_columns) {
                            if (!col->getFunName().empty()) {
                                hasAggFun = true;
                                break;
                            }
                        }

                        for (auto *col : yields_columns) {
                            col->expr()->setContext(expCtx.get());
                            auto status = col->expr()->prepare();
                            if (!status.ok()) {
                                LOG(ERROR) << status.toString();
                                return status;
                            }
                            if (col->alias() == nullptr) {
                                new_columns.emplace_back(col->expr()->toString());
                            } else {
                                new_columns.emplace_back(*col->alias());
                            }
                            if (expCtx->hasVariableProp() || expCtx->hasEdgeProp() || expCtx->hasSrcTagProp() || expCtx->hasDstTagProp()) {
                                return Status::SyntaxError("piped yield `%s' should use variable fields", col->expr()->toString().c_str());
                            }

                            if (hasAggFun) {
                                // check agg
                                if ((col->expr()->isInputExpression() || col->expr()->isVariableExpression())
                                    && col->getFunName().empty()) {
                                    std::string error = "Input columns without aggregation "
                                                        "are not supported in YIELD statement without GROUP BY, near `"
                                                        + col->expr()->toString() + "'";
                                    return Status::SyntaxError(std::move(error));
                                }
                            }

                            //
                            storage::cpp2::YieldColumn y;
                            y.set_fun_name(col->getFunName());
                            if (col->alias()) {
                                y.set_alias(*col->alias());
                            } else {
                                y.set_alias(col->expr()->toString());
                            }
                            y.set_expr(Expression::encode(col->expr()));
                            s.yields.emplace_back(std::move(y));
                        }
                    }

                    for (auto& prop : expCtx->inputProps()) {
                        auto find = std::find(result_columns_.begin(), result_columns_.end(), prop);
                        if (find == result_columns_.end()) {
                            return Status::Error("yiled used input prop `%s' not found.", prop.c_str());
                        }
                    }
                }
                result_columns_ = std::move(new_columns);
                nebula::storage::cpp2::Sentence ss;
                ss.set_yield_sentence(std::move(s));
                sentencesPushDown_.emplace_back(std::move(ss));
            }
                break;
            default:
                return Status::Error("type %d can not pushdown.", static_cast<int>(sentence->kind()));
        }
    }
    return Status::OK();
}

Status GoWholePushDownV2Executor::setupStarts() {
    // Literal vertex ids
    if (!starts_.empty()) {
        auto schemaWriter = std::make_shared<SchemaWriter>();
        schemaWriter->appendCol("id", SupportedType::VID);
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
    if (p_inputs == nullptr || !p_inputs->hasData()) {
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

void GoWholePushDownV2Executor::setupResponse(cpp2::ExecutionResponse &resp) {
    if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    resp = std::move(*resp_);
}

void GoWholePushDownV2Executor::stepOut() {
    auto space = ectx()->rctx()->session()->space();

    auto future  = ectx()->getStorageClient()->getNeighborsWholePushDownV2(
        space, scheme_->toSchema(), std::move(starts_), vid_idx_, sentencesPushDown_);

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
            LOG(INFO) << "GoWholePushDownV2Executor finished, total request vertices " << starts_.size();
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

void GoWholePushDownV2Executor::onStepOutResponse(RpcResponse &&rpcResp) {
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
                SupportedType type(SupportedType::UNKNOWN);
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
        onResult_(std::move(result));
    } else {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(result_columns_);
        resp_->set_rows(std::move(rows));
    }

    doFinish(Executor::ProcessControl::kNext);
}

void GoWholePushDownV2Executor::onEmptyInputs() {
    auto cols = result_columns_;
    auto outputs = std::make_unique<InterimResult>(std::move(cols));
    if (onResult_) {
        onResult_(std::move(outputs));
    } else if (resp_ == nullptr) {
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
    }
    doFinish(Executor::ProcessControl::kNext);
}

}   // namespace graph
}   // namespace nebula
