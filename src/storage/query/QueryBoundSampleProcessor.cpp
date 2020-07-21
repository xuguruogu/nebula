/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "parser/Clauses.h"
#include "storage/query/QueryBoundSampleProcessor.h"

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
    VLOG(3) << "Unknown ColumnType: " << static_cast<int32_t>(value.getType());
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

void QueryBoundSampleProcessor::process(const cpp2::GetNeighborsSampleRequest& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();

    // inputs
    inputs_ = std::move(const_cast<std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>>&>(req.get_parts()));

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

cpp2::ErrorCode QueryBoundSampleProcessor::buildContexts(const cpp2::GetNeighborsSampleRequest& req) {
    expCtx_ = std::make_unique<ExpressionContext>();

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

    // order by

    const auto& orderBy = req.get_sample_filter().get_order_by();
    if (!orderBy.empty()) {
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(orderBy);
        if (!expRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }

        orderBy_ = std::move(expRet).value();
        orderBy_->setContext(expCtx_.get());
    }

    // limit
    limitSize_ = req.get_sample_filter().get_limit_size();

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

    // vidIndex
    vidIndex_ = req.get_vid_index();

    return cpp2::ErrorCode::SUCCEEDED;
}

int32_t QueryBoundSampleProcessor::getBucketsNum(int32_t verticesNum, int32_t minVerticesPerBucket,int32_t handlerNum) {
    return std::min(std::max(1, verticesNum/minVerticesPerBucket), handlerNum);
}

std::vector<QueryBoundSampleProcessor::BucketWithIndex>
QueryBoundSampleProcessor::genBuckets() {
    std::vector<BucketWithIndex> buckets;
    int32_t verticesNum = 0;
    for (auto& pv : inputs_) {
        verticesNum += pv.second.size();
    }
    auto bucketsNum = getBucketsNum(
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

folly::Future<std::vector<OneVertexResp>>
QueryBoundSampleProcessor::asyncProcessBucket(BucketWithIndex bucket) {
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

kvstore::ResultCode QueryBoundSampleProcessor::processVertex(PartitionID partId, VertexID vId, IndexID idx) {
    // input row
    nebula::graph::cpp2::RowValue& input_row = inputs_[partId].at(idx);

    // tag props
    std::vector<std::string> srcTagContents;
    std::unordered_map<TagID, std::unique_ptr<RowReader>> srcTagReaderMap;
    std::unordered_map<std::string, nebula::graph::cpp2::ColumnValue> yieldVariableMap;


    // TODO: add multiple version check
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

    // TODO: add multiple version check
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
            getters.getInputProp = [this,&yieldVariableMap, &input_row] (const std::string& prop) -> OptVariantType {
                auto propIdx= this->input_schema_->getFieldIndex(prop);
                if (propIdx > 0) {
                    auto& col = input_row.get_columns().at(propIdx);
                    return toVariantType(col);
                }

                if (yieldVariableMap.find(prop) != yieldVariableMap.end()) {
                    auto& col = yieldVariableMap[prop];
                    return toVariantType(col);;
                }
                return Status::Error("Input Prop `%s' not found.", prop.c_str());
            };

            getters.getVariableProp = [this, &yieldVariableMap, &input_row] (const std::string& prop) -> OptVariantType {
                auto propIdx= this->input_schema_->getFieldIndex(prop);
                if (propIdx > 0) {
                    auto& col = input_row.get_columns().at(propIdx);
                    return toVariantType(col);
                }

                if (yieldVariableMap.find(prop) != yieldVariableMap.end()) {
                    auto& col = yieldVariableMap[prop];
                    return toVariantType(col);;
                }
                return Status::Error("Input Prop `%s' not found.", prop.c_str());
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

            // yield expr
            nebula::graph::cpp2::RowValue row;
            std::vector<nebula::graph::cpp2::ColumnValue> resultRow;
            resultRow.reserve(yields_.size());
            for (uint32_t index=0 ; index < yields_expr_.size(); index ++) {
                auto& expr = yields_expr_[index];
                if (!checkExp(expr.get())) {
                    VLOG(3) << "check order by fail";
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto exprStatus = expr->eval(getters);
                if (!exprStatus.ok()) {
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto col = toColumnValue(std::move(exprStatus).value());

                yieldVariableMap[yields_[index].get_alias()] = col;

                resultRow.push_back(std::move(col));
            }
            row.set_columns(std::move(resultRow));


            if (orderBy_) {
                if (!checkExp(orderBy_.get())) {
                    VLOG(3) << "check order by fail";
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto value  = orderBy_->eval(getters);
                if (!value.ok()) {
                    // may be out of limitation of double
                    VLOG(3) << "get the orderby value fail";
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto r = value.value();

                if (!(Expression::isInt(r) || Expression::isDouble(r))) {
                    VLOG(3)<< "get the orderby value fail";
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto priorityValue = Expression::asDouble(r);

                if(limitSize_ <= 0){
                    continue;
                }

                std::lock_guard<std::mutex> lg(this->lock_);
                if(resultNodeHeap_.size() >= (uint64_t)limitSize_ && (priorityValue <= resultNodeHeap_.top().priority)) {
                    continue;
                } else if (resultNodeHeap_.size() >= (uint64_t)limitSize_ && priorityValue > resultNodeHeap_.top().priority){
                    resultNodeHeap_.pop();
                }
                // if minHeapNode.size() < (uint64_t)limitSize_
                Node node;
                node.priority = priorityValue;
                node.row = row;
                resultNodeHeap_.push(node);
            } else {
                // order by must be provided
                return kvstore::ResultCode::ERR_INVALID_ARGUMENT;
            }

        } // end iter

    }

    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundSampleProcessor::onProcessFinished() {
    // collect the result
    uint64_t num = 0;
    std::vector<nebula::graph::cpp2::RowValue> rows;
    num = resultNodeHeap_.size();

    rows.reserve(num);
    while( !resultNodeHeap_.empty()) {
        Node node = resultNodeHeap_.top();
        resultNodeHeap_.pop();
        rows.push_back(std::move(node.row));
    }

    std::vector<std::string> columns_names;
    {
        columns_names.reserve(yields_.size());
        for (auto& yield : yields_) {
            columns_names.emplace_back(yield.alias);
        }
    }
    // set resp
    resp_.set_rows(std::move(rows));
}

bool QueryBoundSampleProcessor::checkExp(const Expression* exp) {
    switch (exp->kind()) {
        case Expression::kPrimary:
            return true;
        case Expression::kFunctionCall: {
            auto* funcExp = static_cast<FunctionCallExpression*>(
                const_cast<Expression*>(exp));
            auto* name = funcExp->name();
            auto args = funcExp->args();
            auto func = FunctionManager::get(*name, args.size());
            if (!func.ok()) {
                return false;
            }
            for (auto& arg : args) {
                if (!checkExp(arg)) {
                    return false;
                }
            }
            funcExp->setFunc(std::move(func).value());
            return true;
        }
        case Expression::kUnary: {
            auto* unaExp = static_cast<const UnaryExpression*>(exp);
            return checkExp(unaExp->operand());
        }
        case Expression::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return checkExp(typExp->operand());
        }
        case Expression::kArithmetic: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            return checkExp(ariExp->left()) && checkExp(ariExp->right());
        }
        case Expression::kRelational: {
            auto* relExp = static_cast<const RelationalExpression*>(exp);
            return checkExp(relExp->left()) && checkExp(relExp->right());
        }
        case Expression::kLogical: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            return checkExp(logExp->left()) && checkExp(logExp->right());
        }
        case Expression::kSourceProp: {
            auto* sourceExp = static_cast<const SourcePropertyExpression*>(exp);
            const auto* tagName = sourceExp->alias();
            const auto* propName = sourceExp->prop();
            auto tagRet = this->schemaMan_->toTagID(spaceId_, *tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << *tagName << ", in space " << spaceId_;
                return false;
            }
            auto tagId = tagRet.value();
            // TODO(heng): Now we use the latest version.
            auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
            CHECK(!!schema);
            auto field = schema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on tag " << tagName;
                return false;
            }
            if(srcTagMap_.find(*tagName) == srcTagMap_.end()) {
                VLOG(1) << "There is no related tag existed in srcTagMap!";
                srcTagMap_[*tagName] = tagId;
                src_tag_schema_[tagId] = schema;
            }
            return true;
        }
        case Expression::kEdgeRank:
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId:
        case Expression::kEdgeType: {
            return true;
        }
        case Expression::kAliasProp: {
            if (edges_.size() == 0) {
                VLOG(1) << "No edge requested!";
                return false;
            }

            auto* edgeExp = static_cast<const AliasPropertyExpression*>(exp);

            // TODO(simon.liu) we need handle rename.
            auto edgeStatus = this->schemaMan_->toEdgeType(spaceId_, *edgeExp->alias());
            if (!edgeStatus.ok()) {
                VLOG(1) << "Can't find edge " << *(edgeExp->alias());
                return false;
            }

            auto edgeType = edgeStatus.value();
            auto schema = this->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!schema) {
                VLOG(1) << "Cant find edgeType " << edgeType;
                return false;
            }

            const auto* propName = edgeExp->prop();
            auto field = schema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on edge "
                        << *(edgeExp->alias());
                return false;
            }
            return true;
        }
        case Expression::kVariableProp:
        case Expression::kDestProp:
        case Expression::kInputProp: {
            return true;
        }
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return false;
        }
    }
}
}  // namespace storage
}  // namespace nebula