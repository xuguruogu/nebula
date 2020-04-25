/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "graph/AggregateFunction.h"
#include "parser/Clauses.h"
#include "storage/query/QueryBoundWholePushDownV2Processor.h"

namespace nebula {
namespace storage {

static OptVariantType toVariantType(const nebula::graph::cpp2::ColumnValue& value) {
    switch (value.getType()) {
        case nebula::graph::cpp2::ColumnValue::Type::id:
            return value.get_id();
        case nebula::graph::cpp2::ColumnValue::Type::integer:
            return value.get_integer();
        case nebula::graph::cpp2::ColumnValue::Type::bool_val:
            return value.get_bool_val();
        case nebula::graph::cpp2::ColumnValue::Type::double_precision:
            return value.get_double_precision();
        case nebula::graph::cpp2::ColumnValue::Type::str:
            return value.get_str();
        case nebula::graph::cpp2::ColumnValue::Type::timestamp:
            return value.get_timestamp();
        default:
            break;
    }

    LOG(ERROR) << "Unknown ColumnType: " << static_cast<int32_t>(value.getType());
    return Status::Error("Unknown ColumnType: %d", static_cast<int32_t>(value.getType()));
}

static nebula::graph::cpp2::ColumnValue toColumnValue(const VariantType& value) {
    nebula::graph::cpp2::ColumnValue r;
    switch (value.which()) {
        case VAR_INT64:
            r.set_integer(boost::get<int64_t>(value));
            break;
        case VAR_DOUBLE:
            r.set_double_precision(boost::get<double>(value));
            break;
        case VAR_BOOL:
            r.set_bool_val(boost::get<bool>(value));
            break;
        case VAR_STR:
            r.set_str(boost::get<std::string>(value));
            break;
        default:
            LOG(FATAL) << "Unknown VariantType: " << value.which();
    }

    return r;
}

kvstore::ResultCode QueryBoundWholePushDownV2Processor::processVertex(PartitionID partId, VertexID vId, IndexID idx) {
    // input row
    nebula::graph::cpp2::RowValue& input_row = inputs_[partId].at(idx);

    // tag props
    std::vector<std::string> srcTagContents;
    std::unordered_map<TagID, std::unique_ptr<RowReader>> srcTagReaderMap;

    for (auto& src_tag_schema : src_tag_schema_) {
        auto srcTagId = src_tag_schema.first;

        auto schema = this->schemaMan_->getTagSchema(spaceId_, srcTagId);
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            auto result = vertexCache_->get(std::make_pair(vId, srcTagId), partId);
            if (result.ok()) {
                srcTagContents.emplace_back(std::move(result).value());
                auto& v = srcTagContents.back();
                auto srcTagReader = RowReader::getTagPropReader(this->schemaMan_, v, spaceId_, srcTagId);
                if (srcTagReader == nullptr) {
                    return kvstore::ResultCode::ERR_CORRUPT_DATA;
                }

                srcTagReaderMap[srcTagId] = std::move(srcTagReader);
                VLOG(3) << "Hit cache for vId " << vId << ", srcTagId " << srcTagId;
                continue;
            } else {
                VLOG(3) << "Miss cache for vId " << vId << ", srcTagId " << srcTagId;
            }
        }

        auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, srcTagId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
            return ret;
        }

        if (iter && iter->valid()) {
            auto srcTagReader = RowReader::getTagPropReader(this->schemaMan_, iter->val(), spaceId_, srcTagId);
            if (srcTagReader == nullptr) {
                return kvstore::ResultCode::ERR_CORRUPT_DATA;
            }
            srcTagReaderMap[srcTagId] = std::move(srcTagReader);
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                vertexCache_->insert(std::make_pair(vId, srcTagId), iter->val().str(), partId);
                VLOG(3) << "Insert cache for vId " << vId << ", srcTagId " << srcTagId;
            }
        } else {
            VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", srcTagId " << srcTagId;
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
    }

    // edge props
    for (auto edgeType : edges_) {
        auto prefix = NebulaKeyUtils::edgePrefix(partId, vId, edgeType);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
            return ret;
        }

        EdgeRanking lastRank  = -1;
        VertexID    lastDstId = 0;
        bool        firstLoop = true;
        int         cnt = 0;

        auto schema = this->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
        for (; iter->valid(); iter->next()) {
            if (!FLAGS_enable_reservoir_sampling
                && !(cnt < FLAGS_max_edge_returned_per_vertex)) {
                break;
            }
            auto key = iter->key();
            auto val = iter->val();

            auto rank = NebulaKeyUtils::getRank(key);
            auto dstId = NebulaKeyUtils::getDstId(key);

            if (!firstLoop && rank == lastRank && lastDstId == dstId) {
                VLOG(3) << "Only get the latest version for each edge.";
                continue;
            }
            lastRank = rank;
            lastDstId = dstId;

            ++cnt;
            if (firstLoop) {
                firstLoop = false;
            }

            // edge reader
            auto edgeReader = RowReader::getEdgePropReader(this->schemaMan_, val, spaceId_, edgeType);
            if (edgeReader == nullptr) {
                return kvstore::ResultCode::ERR_CORRUPT_DATA;
            }
            // getters
            Getters getters;
            getters.getEdgeRank = [&rank] () -> VariantType {
                return rank;
            };
            getters.getEdgeDstId = [this, &edgeType, &dstId] (const std::string& edgeName) -> OptVariantType {
                auto edgeFound = this->edgeMap_.find(edgeName);
                if (edgeFound == edgeMap_.end()) {
                    return Status::Error(
                        "Edge `%s' not found when call getters.", edgeName.c_str());
                }
                if (std::abs(edgeType) != edgeFound->second) {
                    return Status::Error("Ignore this edge");
                }
                return dstId;
            };
            getters.getDstTagProp = [] (const std::string&, const std::string&) -> OptVariantType {
                return Status::Error("try access dst prop.");
            };
            getters.getInputProp = [this, &input_row] (const std::string& prop) -> OptVariantType {
                auto propIdx= this->input_schema_->getFieldIndex(prop);
                if (propIdx < 0) {
                    return Status::Error("Input Prop `%s' not found.", prop.c_str());
                }
                auto& col = input_row.get_columns().at(propIdx);
                return toVariantType(col);
            };
            getters.getVariableProp = [this, &input_row] (const std::string& prop) -> OptVariantType {
                auto propIdx= this->input_schema_->getFieldIndex(prop);
                if (propIdx < 0) {
                    return Status::Error("Input Prop `%s' not found.", prop.c_str());
                }
                auto& col = input_row.get_columns().at(propIdx);
                return toVariantType(col);
            };
            getters.getSrcTagProp = [this, &srcTagReaderMap, vId] (const std::string& tag, const std::string& prop) -> OptVariantType {
                auto srcTagFound = this->srcTagMap_.find(tag);
                if (srcTagFound == srcTagMap_.end()) {
                    return Status::Error(
                        "Src tag `%s' not found when call getters.", tag.c_str());
                }
                TagID srcTagId = srcTagFound->second;

                auto srcTagReaderFound = srcTagReaderMap.find(srcTagId);
                if (srcTagReaderFound == srcTagReaderMap.end()) {
                    return Status::Error(
                        "Src tag `%s' not found when call getters.", tag.c_str());
                }
                auto& srcTagReader = srcTagReaderFound->second;

                if (prop == _ID) {
                    return vId;
                }

                auto res = RowReader::getPropByName(srcTagReader.get(), prop);
                if (!ok(res)) {
                    return Status::Error("Invalid src tag prop");
                }
                return value(std::move(res));
            };

            getters.getAliasProp = [this, edgeType, &edgeReader, &key](const std::string& edgeName,
                                                                   const std::string& prop) -> OptVariantType {
                auto edgeFound = this->edgeMap_.find(edgeName);
                if (edgeFound == edgeMap_.end()) {
                    return Status::Error(
                        "Edge `%s' not found when call getters.", edgeName.c_str());
                }
                if (std::abs(edgeType) != edgeFound->second) {
                    return Status::Error("Ignore this edge");
                }

                if (prop == _SRC) {
                    return NebulaKeyUtils::getSrcId(key);
                } else if (prop == _DST) {
                    return NebulaKeyUtils::getDstId(key);
                } else if (prop == _RANK) {
                    return NebulaKeyUtils::getRank(key);
                } else if (prop == _TYPE) {
                    return static_cast<int64_t>(NebulaKeyUtils::getEdgeType(key));
                }

                auto res = RowReader::getPropByName(edgeReader.get(), prop);
                if (!ok(res)) {
                    return Status::Error("Invalid Edge Prop");
                }
                return value(std::move(res));
            };

            // filter
            if (filter_) {
                auto filter_value = filter_->eval(getters);
                if (filter_value.ok() && !Expression::asBool(filter_value.value())) {
                    VLOG(1) << "Filter the edge "
                            << vId << "-> " << dstId << "@" << rank << ":" << edgeType;
                    continue;
                }
            }

            // yield expr
            nebula::graph::cpp2::RowValue row;
            std::vector<nebula::graph::cpp2::ColumnValue> resultRow;
            resultRow.reserve(yields_expr_.size());
            for (auto& expr : yields_expr_) {
                auto exprStatus = expr->eval(getters);
                if (!exprStatus.ok()) {
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto col = toColumnValue(std::move(exprStatus).value());
                resultRow.push_back(std::move(col));
            }

            row.set_columns(std::move(resultRow));
            go_result_[partId][idx].push_back(std::move(row));
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundWholePushDownV2Processor::onProcessFinished() {
    // collcet
    uint64_t num = 0;
    std::vector<nebula::graph::cpp2::RowValue> rows;
    for (auto& part : go_result_) {
        for (auto& list : part.second) {
            num += list.size();
        }
    }

    rows.reserve(num);
    for (auto& part : go_result_) {
        for (auto& list : part.second) {
            rows.insert(rows.end(), std::make_move_iterator(list.begin()), std::make_move_iterator(list.end()));
        }
    }
    std::vector<std::string> columns_names;
    {
        columns_names.reserve(yields_.size());
        for (auto& yield : yields_) {
            columns_names.emplace_back(yield.alias);
        }
    }

    // sub_sentences
    for (unsigned i = 1; i < sub_sentences_.size(); i++) {
        auto& sentence = sub_sentences_[i];
        switch (sentence.getType()) {
            case cpp2::Sentence::Type::order_by_sentence:
            {
                auto& order_by_sentence = sentence.get_order_by_sentence();
                auto& orderByFactors = order_by_sentence.get_order_by();

                auto comparator = [&orderByFactors] (::nebula::graph::cpp2::RowValue& lhs, ::nebula::graph::cpp2::RowValue& rhs) {
                    const auto &lhsColumns = lhs.get_columns();
                    const auto &rhsColumns = rhs.get_columns();
                    for (auto &factor : orderByFactors) {
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
                std::sort(rows.begin(), rows.end(), comparator);
            }
                break;
            case cpp2::Sentence::Type::limit_sentence:
            {
                auto& limit_sentence = sentence.get_limit_sentence();
                auto cnt = static_cast<uint64_t >(limit_sentence.get_offset() + limit_sentence.get_count());
                if (rows.size() > cnt) {
                    rows.resize(cnt);
                }
            }
                break;
            case cpp2::Sentence::Type::group_by_sentence:
            {
                auto& group_by_sentence = sentence.get_group_by_sentence();
                auto& group_columns = group_by_sentence.get_group();
                auto& yields_columns = group_by_sentence.get_yields();
                std::vector<std::unique_ptr<Expression>> group_columns_expr;
                std::vector<std::unique_ptr<Expression>> yields_columns_expr;
                {
                    group_columns_expr.reserve(group_columns.size());
                    for (auto& group : group_columns) {
                        group_columns_expr.emplace_back(Expression::decode(group.get_expr()).value());
                    }
                    yields_columns_expr.reserve(yields_columns.size());
                    for (auto& yield : yields_columns) {
                        yields_columns_expr.emplace_back(Expression::decode(yield.get_expr()).value());
                    }
                }
                std::unordered_map<std::string, unsigned > schemaMap;
                {
                    for (unsigned idx = 0; idx < columns_names.size(); idx++) {
                        schemaMap[columns_names[idx]] = idx;
                    }
                }

                using FunCols = std::vector<std::shared_ptr<graph::AggFun>>;
                // key : the column values of group by, val: function table of aggregated columns
                using GroupData = std::unordered_map<graph::ColVals, FunCols, graph::ColsHasher>;

                GroupData data;
                Getters getters;
                for (auto& it : rows) {
                    graph::ColVals groupVals;
                    FunCols calVals;
                    // Firstly: group the cols
                    for (unsigned g_idx = 0; g_idx < group_columns.size(); g_idx++) {
                        // auto& col = group_columns[g_idx];
                        auto& col_expr = group_columns_expr[g_idx];
                        getters.getInputProp = [&] (const std::string & prop) -> OptVariantType {
                            auto indexIt = schemaMap.find(prop);
                            if (indexIt == schemaMap.end()) {
                                LOG(ERROR) << prop <<  " is nonexistent";
                                return Status::Error("%s is nonexistent", prop.c_str());
                            }
                            auto val = it.columns[indexIt->second];
                            return toVariantType(val);
                        };
                        groupVals.vec.emplace_back(toColumnValue(col_expr->eval(getters).value()));
                    }

                    auto findIt = data.find(groupVals);

                    // Secondly: get the value of the aggregated column

                    // Get all aggregation function
                    if (findIt == data.end()) {
                        for (unsigned y_idx = 0; y_idx < yields_columns.size(); y_idx++) {
                            auto& yield = yields_columns[y_idx];
                            // auto& yield_expr = yields_columns_expr[y_idx];
                            auto funPtr = graph::funVec[yield.get_fun_name()]();
                            calVals.emplace_back(std::move(funPtr));
                        }
                    } else {
                        calVals = findIt->second;
                    }

                    // Apply value
                    for (unsigned y_idx = 0; y_idx < yields_columns.size(); y_idx++) {
                        // auto& yield = yields_columns[y_idx];
                        auto& yield_expr = yields_columns_expr[y_idx];
                        getters.getInputProp = [&] (const std::string &prop) -> OptVariantType{
                            auto indexIt = schemaMap.find(prop);
                            if (indexIt == schemaMap.end()) {
                                LOG(ERROR) << prop <<  " is nonexistent";
                                return Status::Error("%s is nonexistent", prop.c_str());
                            }
                            auto val = it.columns[indexIt->second];
                            return toVariantType(val);
                        };
                        auto v = toColumnValue(yield_expr->eval(getters).value());
                        calVals[y_idx]->apply(v);
                    }

                    if (findIt == data.end()) {
                        data.emplace(std::move(groupVals), std::move(calVals));
                    }
                }

                // Generate result data
                rows.clear();
                rows.reserve(data.size());
                for (auto& item : data) {
                    std::vector<graph::cpp2::ColumnValue> row;
                    for (auto& col : item.second) {
                        row.emplace_back(col->getResult());
                    }
                    rows.emplace_back();
                    rows.back().set_columns(std::move(row));
                }
                columns_names.clear();
                columns_names.reserve(yields_columns.size());
                for (auto& yield : yields_columns) {
                    columns_names.emplace_back(yield.get_alias());
                }
            }
                break;
            case cpp2::Sentence::Type::yield_sentence:
            {
                auto& yield_sentence = sentence.get_yield_sentence();
                std::unique_ptr<Expression> filter;
                if (!yield_sentence.get_where().empty())
                    filter = Expression::decode(yield_sentence.get_where()).value();
                auto& yields_columns = yield_sentence.get_yields();
                std::vector<std::unique_ptr<Expression>> yields_columns_expr;

                bool hasAggFun = false;
                std::vector<std::shared_ptr<graph::AggFun>> aggFuns;
                {
                    yields_columns_expr.reserve(yields_columns.size());
                    for (auto& yield : yields_columns) {
                        yields_columns_expr.push_back(Expression::decode(yield.get_expr()).value());
                        if (!yield.get_fun_name().empty()) {
                            hasAggFun = true;
                            aggFuns.emplace_back(graph::funVec[yield.get_fun_name()]());
                        }
                    }
                }
                if (hasAggFun && aggFuns.size() != yields_columns.size()) {
                    LOG(ERROR) << "agg funcs size not match yields columns size.";
                    throw std::logic_error("agg funcs size not match yields columns size.");
                }
                std::unordered_map<std::string, unsigned > schemaMap;
                {
                    for (unsigned idx = 0; idx < columns_names.size(); idx++) {
                        schemaMap[columns_names[idx]] = idx;
                    }
                }

                Getters getters;
                std::vector<nebula::graph::cpp2::RowValue> new_rows;
                {
                    if (!hasAggFun) {
                        new_rows.reserve(rows.size());
                    }
                }

                for (auto& row : rows) {
                    getters.getInputProp = [&] (const std::string & prop) -> OptVariantType {
                        auto indexIt = schemaMap.find(prop);
                        if (indexIt == schemaMap.end()) {
                            LOG(ERROR) << prop <<  " is nonexistent";
                            return Status::Error("%s is nonexistent", prop.c_str());
                        }
                        auto val = row.columns[indexIt->second];
                        return toVariantType(val);
                    };

                    if (filter && !Expression::asBool(filter->eval(getters).value()))
                        continue;

                    if (hasAggFun) {
                        for (unsigned y_idx = 0; y_idx < yields_columns.size(); y_idx++) {
                            auto& col_expr = yields_columns_expr[y_idx];
                            auto v = toColumnValue(col_expr->eval(getters).value());
                            aggFuns[y_idx]->apply(v);
                        }
                    } else {
                        std::vector<graph::cpp2::ColumnValue> new_row;
                        new_row.reserve(yields_columns.size());
                        for (unsigned y_idx = 0; y_idx < yields_columns.size(); y_idx++) {
                            auto& col_expr = yields_columns_expr[y_idx];
                            auto v = toColumnValue(col_expr->eval(getters).value());
                            new_row.emplace_back(v);
                        }
                        graph::cpp2::RowValue r;
                        r.set_columns(std::move(new_row));
                        new_rows.emplace_back(std::move(r));
                    }
                }

                if (hasAggFun) {
                    std::vector<graph::cpp2::ColumnValue> new_row;
                    for (auto& agg : aggFuns) {
                        new_row.emplace_back(agg->getResult());
                    }
                    graph::cpp2::RowValue r;
                    r.set_columns(std::move(new_row));
                    new_rows.emplace_back(std::move(r));
                }

                rows = std::move(new_rows);

                columns_names.clear();
                columns_names.reserve(yields_columns.size());
                for (auto& yield : yields_columns) {
                    columns_names.emplace_back(yield.get_alias());
                }
            }
                break;
            default:
                break;
        }
    }

    // set resp
    resp_.set_rows(std::move(rows));
}

cpp2::ErrorCode QueryBoundWholePushDownV2Processor::buildContexts(const cpp2::GetNeighborsWholePushDownV2Request& req) {
    expCtx_ = std::make_unique<ExpressionContext>();
    // sub_sentences
    sub_sentences_ = req.get_sentences();
    if (sub_sentences_.empty() || sub_sentences_[0].getType() != cpp2::Sentence::Type::go_sentence) {
        return cpp2::ErrorCode::E_INVALID_SENTENCE;
    }
    auto& go_sentence = sub_sentences_[0].get_go_sentence();

    for (unsigned i = 1; i < sub_sentences_.size(); i++) {
        if (sub_sentences_[i].getType() == cpp2::Sentence::Type::go_sentence
            || sub_sentences_[i].getType() == cpp2::Sentence::Type::go_sentence) {
            return cpp2::ErrorCode::E_INVALID_SENTENCE;
        }
    }

    // filter
    {
        const auto& filterStr = go_sentence.get_where();
        if (!filterStr.empty()) {
            StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(filterStr);
            if (!expRet.ok()) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
            filter_ = std::move(expRet).value();
            filter_->setContext(expCtx_.get());
            Status status = filter_->prepare();
            if (!status.ok()) {
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
        }
    }

    // yields
    {
        yields_ = go_sentence.get_yields();
        yields_expr_.reserve(yields_.size());
        std::unordered_set<std::string> yields_colnames_set;
        for (auto& yield : yields_) {
            StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(yield.get_expr());
            if (!expRet.ok()) {
                return cpp2::ErrorCode::E_INVALID_YIELD;
            }
            auto expr = std::move(expRet).value();
            expr->setContext(expCtx_.get());
            Status status = expr->prepare();
            if (!status.ok()) {
                return cpp2::ErrorCode::E_INVALID_YIELD;
            }
            yields_expr_.push_back(std::move(expr));
            if (yields_colnames_set.find(yield.get_alias()) != yields_colnames_set.end()) {
                return cpp2::ErrorCode::E_DUPLICATE_COLUMNS;
            }
            yields_colnames_set.insert(yield.get_alias());
        }
    }

    // check dst.
    if (expCtx_->hasDstTagProp()) {
        return cpp2::ErrorCode::E_INVALID_DST_PROP;
    }

    // schema
    input_schema_ = std::make_unique<ResultSchemaProvider>(req.schema);

    // src tag schema
    for (auto &src_prop : expCtx_->srcTagProps()) {
        StatusOr<TagID> status = schemaMan_->toTagID(spaceId_, src_prop.first);
        if (!status.ok()) {
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        auto tagId = status.value();
        auto schema = schemaMan_->getTagSchema(spaceId_, tagId);
        if (!schema) {
            VLOG(3) << "Can't find spaceId " << spaceId_ << ", tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        src_tag_schema_[tagId] = schema;
        srcTagMap_[src_prop.first] = tagId;
    }

    // edge schema
    for (auto &alias_prop : expCtx_->aliasProps()) {
        StatusOr<EdgeType> status = schemaMan_->toEdgeType(spaceId_, alias_prop.first);
        if (!status.ok()) {
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        auto edge = status.value();
        auto schema = schemaMan_->getEdgeSchema(spaceId_, edge);
        if (!schema) {
            VLOG(3) << "Can't find spaceId " << spaceId_ << ", edge " << edge;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        edge_schema_[edge] = schema;
        edgeMap_[alias_prop.first] = edge;
    }

    // edge types
    edges_ = go_sentence.get_edge_types();

    // vidIndex
    vidIndex_ = req.get_vid_index();

    // sub_sentences
    for (unsigned i = 1; i < sub_sentences_.size(); i++) {
        auto& sentence = sub_sentences_[i];
        switch (sentence.getType()) {
            case cpp2::Sentence::Type::limit_sentence:
            {
                auto& limit_sentence = sentence.get_limit_sentence();
                if (limit_sentence.get_offset() < 0 || limit_sentence.get_count() < 0) {
                    return cpp2::ErrorCode::E_INVALID_SENTENCE;
                }
            }
                break;
            case cpp2::Sentence::Type::group_by_sentence:
            case cpp2::Sentence::Type::order_by_sentence:
            case cpp2::Sentence::Type::yield_sentence:
                break;
            default:
                return cpp2::ErrorCode::E_INVALID_SENTENCE;
        }
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<std::vector<OneVertexResp>>
QueryBoundWholePushDownV2Processor::asyncProcessBucket(BucketWithIndex bucket) {
    folly::Promise<std::vector<OneVertexResp>> pro;
    auto f = pro.getFuture();
    executor_->add([this, p = std::move(pro), b = std::move(bucket)] () mutable {
        std::vector<OneVertexResp> codes;
        codes.reserve(b.vertices_.size());
        for (auto& pv : b.vertices_) {
            codes.emplace_back(std::get<0>(pv),
                               std::get<1>(pv),
                               processVertex(std::get<0>(pv), std::get<1>(pv), std::get<2>(pv)));
        }
        p.setValue(std::move(codes));
    });
    return f;
}

std::vector<QueryBoundWholePushDownV2Processor::BucketWithIndex>
QueryBoundWholePushDownV2Processor::genBuckets() {
    std::vector<BucketWithIndex> buckets;
    int32_t verticesNum = 0;
    for (auto& pv : inputs_) {
        verticesNum += pv.second.size();
    }
    auto bucketsNum = detail::getBucketsNum(
        verticesNum,
        FLAGS_min_vertices_per_bucket,
        FLAGS_max_handlers_per_req);

    buckets.resize(bucketsNum);
    auto vNumPerBucket = verticesNum / bucketsNum;
    auto leftVertices = verticesNum % bucketsNum;
    int32_t bucketIndex = -1;
    size_t thresHold = vNumPerBucket;

    for (auto& pv : inputs_) {
        auto& rows = pv.second;
        for (unsigned row_idx = 0; row_idx < rows.size(); row_idx++) {
            auto& row = rows[row_idx];
            VertexID vId;
            const nebula::graph::cpp2::ColumnValue& col = row.get_columns().at(vidIndex_);
            switch (col.getType()) {
                case nebula::graph::cpp2::ColumnValue::Type::id:
                    vId = col.get_id();
                    break;
                case nebula::graph::cpp2::ColumnValue::Type::integer:
                    vId = col.get_integer();
                    break;
                default:
                    continue;
            }

            if (bucketIndex < 0 || buckets[bucketIndex].vertices_.size() >= thresHold) {
                ++bucketIndex;
                thresHold = bucketIndex < leftVertices ? vNumPerBucket + 1 : vNumPerBucket;
                buckets[bucketIndex].vertices_.reserve(thresHold);
            }
            CHECK_LT(bucketIndex, bucketsNum);
            buckets[bucketIndex].vertices_.emplace_back(pv.first, vId, row_idx);
        }
    }
    return buckets;
}

void QueryBoundWholePushDownV2Processor::process(const cpp2::GetNeighborsWholePushDownV2Request& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();

    // inputs
    inputs_ = std::move(const_cast<std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>>&>(req.get_parts()));
    for (auto& part : inputs_) {
        auto partId = part.first;
        auto& rows = part.second;
        auto& result = go_result_[partId];
        result.resize(rows.size());
    }

    auto retCode = buildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    auto buckets = genBuckets();
    std::vector<folly::Future<std::vector<OneVertexResp>>> results;
    for (auto& bucket : buckets) {
        results.emplace_back(asyncProcessBucket(std::move(bucket)));
    }

    folly::collectAll(results).via(executor_).thenTry([this] (auto&& t) mutable {
        CHECK(!t.hasException());
        std::unordered_set<PartitionID> failedParts;
        for (auto& bucketTry : t.value()) {
            CHECK(!bucketTry.hasException());
            for (auto& r : bucketTry.value()) {
                auto& partId = std::get<0>(r);
                auto& ret = std::get<2>(r);
                if (ret != kvstore::ResultCode::SUCCEEDED
                    && failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                        this->handleLeaderChanged(spaceId_, partId);
                    } else {
                        this->pushResultCode(this->to(ret), partId);
                    }
                }
            }
        }
        this->onProcessFinished();
        this->onFinished();
    });
}

}  // namespace storage
}  // namespace nebula
