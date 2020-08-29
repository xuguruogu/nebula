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

void QueryBoundSampleProcessor::process(const cpp2::GetNeighborsSampleRequest& reqConst) {
    auto& req = const_cast<cpp2::GetNeighborsSampleRequest&>(reqConst);
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();

    // inputs
    inputs_ = std::move(req.get_parts());
    for (auto& part : inputs_) {
        auto partId = part.first;
        auto& rows = part.second;
        auto& result = sample_result_[partId];
        result.resize(rows.size());
    }

    auto retCode = buildContexts(req);

    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : inputs_) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    auto buckets = genBuckets();

    std::vector<folly::Future<std::vector<OneVertexResp>>> results;
    results.reserve(buckets.size());
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

cpp2::ErrorCode QueryBoundSampleProcessor::buildContexts(cpp2::GetNeighborsSampleRequest& req) {
    expCtx_ = std::make_unique<ExpressionContext>();
    // yields
    {
        yields_ = std::move(req.get_yields());
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
            if (!checkExp(expr.get())) {
                VLOG(3) << "check yeild fail";
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
    {
        sampleOrderBy_ = std::move(req.get_sample_filter().get_order_by());
        if (sampleOrderBy_.empty()) {
            VLOG(1) << "check order by fail";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(sampleOrderBy_);
        if (!expRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }

        sampleOrderByExpr_ = std::move(expRet).value();
        sampleOrderByExpr_->setContext(expCtx_.get());
        Status status = sampleOrderByExpr_->prepare();
        if (!status.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!checkExp(sampleOrderByExpr_.get())) {
            VLOG(1) << "check order by fail";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }

    // limit
    {
        sampleLimitSize_ = req.get_sample_filter().get_limit_size();
        if (sampleLimitSize_ <= 0) {
            VLOG(1) << "limit should not <= 0";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }

    // schema
    input_schema_ = std::make_unique<ResultSchemaProvider>(req.schema);

    // src tag schema
    for (auto &src_prop : expCtx_->srcTagProps()) {
        StatusOr<TagID> status = schemaMan_->toTagID(spaceId_, src_prop.first);
        if (!status.ok()) {
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        tags_.emplace_back(status.value());
    }

    // edge schema
    for (auto &alias_prop : expCtx_->aliasProps()) {
        StatusOr<EdgeType> status = schemaMan_->toEdgeType(spaceId_, alias_prop.first);
        if (!status.ok()) {
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
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
            codes.emplace_back(
                std::get<0>(pv),
                std::get<1>(pv),
                processVertex(std::get<0>(pv), std::get<1>(pv), std::get<2>(pv))
            );
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
    std::unordered_map<TagID, RowReader> srcTagReaderMap;
    std::vector<Node> resultNodeHeap;
    resultNodeHeap.reserve(sampleLimitSize_);

    // TODO: add multiple version check
    for (auto srcTagId : tags_) {
        auto schema = this->schemaMan_->getTagSchema(spaceId_, srcTagId);
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            auto result = vertexCache_->get(std::make_pair(vId, srcTagId));
            if (result.ok()) {
                std::pair<TagVersion, folly::Optional<std::string>> v
                    = std::move(result).value();
                // TagVersion tagVersion = v.first;
                folly::Optional<std::string>& row = v.second;
                if (!row.hasValue()) {
                    VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << srcTagId;
                    return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
                } else {
                    srcTagContents.emplace_back(std::move(row.value()));
                    auto srcTagReader = RowReader::getTagPropReader(
                        this->schemaMan_, srcTagContents.back(), spaceId_, srcTagId);
                    if (srcTagReader == nullptr) {
                        return kvstore::ResultCode::ERR_CORRUPT_DATA;
                    }
                    srcTagReaderMap.emplace(srcTagId, std::move(srcTagReader));
                    VLOG(3) << "Hit cache for vId " << vId << ", srcTagId " << srcTagId;
                }
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
            TagVersion tagVersion = NebulaKeyUtils::getVersionBigEndian(iter->key());
            srcTagContents.emplace_back(iter->val());
            auto srcTagReader = RowReader::getTagPropReader(this->schemaMan_, srcTagContents.back(), spaceId_, srcTagId);
            if (srcTagReader == nullptr) {
                return kvstore::ResultCode::ERR_CORRUPT_DATA;
            }
            srcTagReaderMap.emplace(srcTagId, std::move(srcTagReader));
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                vertexCache_->insert(
                    std::make_pair(vId, srcTagId),
                    std::make_pair(tagVersion, folly::Optional<std::string>(iter->val().str()))
                );
                VLOG(3) << "Insert cache for vId " << vId << ", srcTagId " << srcTagId;
            }
        } else {
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                auto tagVersion = folly::Endian::big(
                    std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec());
                vertexCache_->insert(
                    std::make_pair(vId, srcTagId),
                    std::make_pair(tagVersion, folly::Optional<std::string>())
                );
            }
            VLOG(3) << "Missed partId " << partId
                    << ", vId " << vId
                    << ", srcTagId " << srcTagId;
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

        EdgeRanking rank = 0;
        VertexID dstId = 0;
        folly::StringPiece key, val;

        // getters
        Getters getters;
        getters.getEdgeRank = [&rank] () -> VariantType {
            return rank;
        };
        getters.getEdgeDstId = [&dstId] (const std::string&) -> OptVariantType {
            return dstId;
        };
        getters.getInputProp = [this, &input_row] (const std::string& prop) -> OptVariantType {
            auto propIdx= this->input_schema_->getFieldIndex(prop);
            if (propIdx >= 0) {
                auto& col = input_row.get_columns().at(propIdx);
                return toVariantType(col);
            }
            return Status::Error("Input Prop `%s' not found.", prop.c_str());
        };

        getters.getVariableProp = [this, &input_row] (const std::string& prop) -> OptVariantType {
            auto propIdx= this->input_schema_->getFieldIndex(prop);
            if (propIdx >= 0) {
                auto& col = input_row.get_columns().at(propIdx);
                return toVariantType(col);
            }
            return Status::Error("Input Prop `%s' not found.", prop.c_str());
        };
        getters.getSrcTagProp = [this, &srcTagReaderMap, &vId] (const std::string& tag, const std::string& prop) -> OptVariantType {
            StatusOr<TagID> status = schemaMan_->toEdgeType(spaceId_, tag);
            if (!status.ok()) {
                return Status::Error(
                    "Tag `%s' not found when call getters.", tag.c_str());
            }
            TagID srcTagId = status.value();

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

        getters.getAliasProp = [this, &edgeType, &val, &key](const std::string& edgeName,
                                                           const std::string& prop) -> OptVariantType {
            StatusOr<EdgeType> status = schemaMan_->toEdgeType(spaceId_, edgeName);
            if (!status.ok()) {
                return Status::Error(
                    "Edge `%s' not found when call getters.", edgeName.c_str());
            }
            if (std::abs(edgeType) != status.value()) {
                return Status::Error("Ignore this edge");
            }

            if (prop == _SRC) {
                return NebulaKeyUtils::getSrcId(key);
            } else if (prop == _TYPE) {
                return static_cast<int64_t>(NebulaKeyUtils::getEdgeType(key));
            }

            // edge reader
            auto edgeReader = RowReader::getEdgePropReader(this->schemaMan_, val, spaceId_, std::abs(edgeType));
            if (edgeReader == nullptr) {
                return Status::Error("Ignore this edge");
            }

            auto res = RowReader::getPropByName(edgeReader.get(), prop);
            if (!ok(res)) {
                return Status::Error("Invalid Edge Prop");
            }
            return value(std::move(res));
        };

        for (; iter->valid(); iter->next()) {
            key = iter->key();
            val = iter->val();

            rank = NebulaKeyUtils::getRank(key);
            dstId = NebulaKeyUtils::getDstId(key);

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

            auto value  = sampleOrderByExpr_->eval(getters);
            if (!value.ok()) {
                // may be out of limitation of double
                VLOG(3) << "get the orderby value fail";
                return kvstore::ResultCode::ERR_EXPR_FAILED;
            }
            auto r = std::move(value).value();

            if (!(Expression::isInt(r) || Expression::isDouble(r))) {
                VLOG(3)<< "get the orderby value fail";
                return kvstore::ResultCode::ERR_EXPR_FAILED;
            }
            auto score = Expression::asDouble(r);
            if(resultNodeHeap.size() >= static_cast<size_t>(sampleLimitSize_)
               && (score <= resultNodeHeap.front().score)) {
                continue;
            }
            if (resultNodeHeap.size() >= static_cast<size_t>(sampleLimitSize_)
                && score > resultNodeHeap.front().score) {
                std::pop_heap(resultNodeHeap.begin(), resultNodeHeap.end());
                resultNodeHeap.pop_back();
            }

            // yield expr
            nebula::graph::cpp2::RowValue row;
            std::vector<nebula::graph::cpp2::ColumnValue> resultRow;
            resultRow.reserve(yields_.size());
            for (unsigned index = 0 ; index < yields_expr_.size(); index ++) {
                auto& expr = yields_expr_[index];
                auto exprStatus = expr->eval(getters);
                if (!exprStatus.ok()) {
                    VLOG(3) << "check yeild fail";
                    return kvstore::ResultCode::ERR_EXPR_FAILED;
                }
                auto col = toColumnValue(std::move(exprStatus).value());
                resultRow.push_back(std::move(col));
            }
            row.set_columns(std::move(resultRow));

            Node node;
            node.score = score;
            node.row = std::move(row);
            resultNodeHeap.emplace_back(std::move(node));
            std::push_heap(resultNodeHeap.begin(), resultNodeHeap.end());
        } // end iter
    }
    sample_result_[partId][idx]= std::move(resultNodeHeap);
    return kvstore::ResultCode::SUCCEEDED;
}

void QueryBoundSampleProcessor::onProcessFinished() {
    // collect the result
    uint64_t num = 0;
    std::vector<nebula::graph::cpp2::RowValue> rows;
    for (auto& part : sample_result_) {
        for (auto& list : part.second) {
            num += list.size();
        }
    }
    rows.reserve(num);
    for (auto& part : sample_result_) {
        for (auto& list : part.second) {
            for (auto& item : list) {
                rows.push_back(std::move(item.row));
            }
        }
    }

    std::vector<std::string> columns_names;
    columns_names.reserve(yields_.size());
    for (unsigned i = 0; i < yields_.size(); i++) {
        if (!yields_[i].alias.empty()) {
            columns_names.emplace_back(std::move(yields_[i].alias));
        } else {
            columns_names.emplace_back(yields_expr_[i]->toString());
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
        case Expression::kSourceProp:
        case Expression::kEdgeRank:
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId:
        case Expression::kEdgeType:
        case Expression::kAliasProp:
        case Expression::kVariableProp:
        case Expression::kDestProp:
        case Expression::kInputProp:
            return true;
        case Expression::kVariableVariant:
            return false;
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return false;
        }
    }
}
}  // namespace storage
}  // namespace nebula