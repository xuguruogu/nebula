/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryBoundWholePushDownProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {
namespace cpp2 {

bool ColumnValue::operator<(const ColumnValue& rhs) const {
    DCHECK_EQ(type_, rhs.type_);
    auto& lhs = *this;
    switch (lhs.type_) {
        case Type::bool_val: {
            return lhs.value_.bool_val < rhs.value_.bool_val;
        }
        case Type::integer: {
            return lhs.value_.integer < rhs.value_.integer;
        }
        case Type::id: {
            return lhs.value_.id < rhs.value_.id;
        }
        case Type::single_precision: {
            return lhs.value_.single_precision < rhs.value_.single_precision;
        }
        case Type::double_precision: {
            return lhs.value_.double_precision < rhs.value_.double_precision;
        }
        case Type::str: {
            return lhs.value_.str < rhs.value_.str;
        }
        case Type::timestamp: {
            return lhs.value_.timestamp < rhs.value_.timestamp;
        }
        case Type::year: {
            return lhs.value_.year < rhs.value_.year;
        }
        case Type::month: {
            return lhs.value_.month < rhs.value_.month;
        }
        case Type::date: {
            return lhs.value_.date < rhs.value_.date;
        }
        case Type::datetime: {
            return lhs.value_.datetime < rhs.value_.datetime;
        }
        default: {
            return false;
        }
    }
    return false;
}
}   // namespace cpp2
}
}

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

kvstore::ResultCode QueryBoundWholePushDownProcessor::processVertex(PartitionID partId, VertexID vId, IndexID idx) {
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
            result_[partId][idx].push_back(std::move(row));
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundWholePushDownProcessor::onProcessFinished(int32_t retNum) {
    (void)retNum;

    // collcet
    uint64_t num = 0;
    std::vector<nebula::graph::cpp2::RowValue> rows;
    for (auto& part : result_) {
        for (auto& list : part.second) {
            num += list.size();
        }
    }
    rows.reserve(num);
    for (auto& part : result_) {
        for (auto& list : part.second) {
            rows.insert(rows.end(), std::make_move_iterator(list.begin()), std::make_move_iterator(list.end()));
        }
    }

    // order by && limit
    if (!layerOrderBySortFactors_.empty()) {

        auto comparator = [this] (::nebula::graph::cpp2::RowValue& lhs, ::nebula::graph::cpp2::RowValue& rhs) {
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

        if (layer_limit_count_ > 0 && rows.size() > static_cast<uint64_t>(layer_limit_count_)) {
            std::nth_element(rows.begin(), rows.begin() + layer_limit_count_, rows.end(), comparator);
            rows.resize(layer_limit_count_);
        }

        std::sort(rows.begin(), rows.end(), comparator);
    }

    // set resp
    resp_.set_rows(std::move(rows));
}

cpp2::ErrorCode QueryBoundWholePushDownProcessor::buildContexts(const cpp2::GetNeighborsWholePushDownRequest& req) {
    expCtx_ = std::make_unique<ExpressionContext>();
    // filter
    {
        const auto& filterStr = req.get_filter();
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
        yields_ = req.get_yields();
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
    edges_ = req.get_edge_types();

    // layer limit count
    layer_limit_count_ = req.get_layer_limit();
    // order by factors
    layerOrderBySortFactors_ = req.get_order_by();

    // vidIndex
    vidIndex_ = req.get_vid_index();

    return cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<std::vector<OneVertexResp>>
QueryBoundWholePushDownProcessor::asyncProcessBucket(BucketWithIndex bucket) {
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

std::vector<BucketWithIndex>
QueryBoundWholePushDownProcessor::genBuckets() {
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

void QueryBoundWholePushDownProcessor::process(const cpp2::GetNeighborsWholePushDownRequest& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();

    // inputs
    // result
    inputs_ = std::move(const_cast<std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>>&>(req.get_parts()));
    for (auto& part : inputs_) {
        auto partId = part.first;
        auto& rows = part.second;
        auto& result = result_[partId];
        result.resize(rows.size());
    }

    int32_t yieldsNum = req.get_yields().size();
    VLOG(1) << "Total edge types " << req.edge_types.size()
            << ", total yield columns " << yieldsNum
            << ", the first column "
            << (yieldsNum > 0 ? req.get_yields()[0].get_alias() : "");
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", yield cols " << yieldsNum;

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
    folly::collectAll(results).via(executor_).thenTry(
        [this, yieldsNum] (auto&& t) mutable {
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
        this->onProcessFinished(yieldsNum);
        this->onFinished();
    });
}

}  // namespace storage
}  // namespace nebula
