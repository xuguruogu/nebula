/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "storage/query/QueryBaseProcessor.h"

DECLARE_int32(reserved_edges_one_vertex);

namespace nebula {
namespace storage {


struct BucketWithIndex {
    std::vector<std::tuple<PartitionID, VertexID, IndexID>> vertices_;
};

class QueryBoundWholePushDownProcessor
    : public BaseProcessor<cpp2::GetNeighborsWholePushDownResponse> {
//    FRIEND_TEST(QueryBoundTest,  GenBucketsTest);

public:
    static QueryBoundWholePushDownProcessor* instance(kvstore::KVStore* kvstore,
                                                      meta::SchemaManager* schemaMan,
                                                      stats::Stats* stats,
                                                      folly::Executor* executor,
                                                      VertexCache* cache = nullptr) {
        return new QueryBoundWholePushDownProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::GetNeighborsWholePushDownRequest& req);

protected:
    explicit QueryBoundWholePushDownProcessor(kvstore::KVStore* kvstore,
                                meta::SchemaManager* schemaMan,
                                stats::Stats* stats,
                                folly::Executor* executor = nullptr,
                                VertexCache* cache = nullptr)
        : BaseProcessor<cpp2::GetNeighborsWholePushDownResponse>(kvstore, schemaMan, stats)
        , executor_(executor)
        , vertexCache_(cache) {}

    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId, IndexID idx);

    void onProcessFinished(int32_t retNum);

private:
    std::vector<BucketWithIndex> genBuckets();
    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(BucketWithIndex bucket);
    cpp2::ErrorCode buildContexts(const cpp2::GetNeighborsWholePushDownRequest& req);
protected:
    GraphSpaceID  spaceId_;
    std::unique_ptr<ExpressionContext> expCtx_;

    std::unique_ptr<Expression> filter_;
    std::unique_ptr<nebula::meta::SchemaProviderIf> input_schema_;
    std::unordered_map<TagID, std::shared_ptr<const nebula::meta::SchemaProviderIf>> src_tag_schema_;
    std::unordered_map<EdgeType, std::shared_ptr<const nebula::meta::SchemaProviderIf>> edge_schema_;
    std::unordered_map<std::string, EdgeType> edgeMap_;
    std::unordered_map<std::string, TagID> srcTagMap_;

    int32_t vidIndex_{-1};

    int32_t layer_limit_count_{-1};
    std::vector<nebula::storage::cpp2::OrderByFactor>   layerOrderBySortFactors_;

    std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>> inputs_;
    std::unordered_map<PartitionID, std::vector<std::vector<nebula::graph::cpp2::RowValue>>> result_;

    std::vector<nebula::storage::cpp2::YieldColumn> yields_;
    std::vector<std::unique_ptr<nebula::Expression>> yields_expr_;

    std::vector<EdgeType> edges_;

    folly::Executor* executor_{nullptr};
    VertexCache* vertexCache_{nullptr};
};

}  // namespace storage
}  // namespace nebula
