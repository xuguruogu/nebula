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

class QueryBoundWholePushDownV2Processor
    : public BaseProcessor<cpp2::GetNeighborsWholePushDownResponse> {
//    FRIEND_TEST(QueryBoundTest,  GenBucketsTest);

    struct BucketWithIndex {
        std::vector<std::tuple<PartitionID, VertexID, IndexID>> vertices_;
    };
public:
    static QueryBoundWholePushDownV2Processor* instance(kvstore::KVStore* kvstore,
                                                      meta::SchemaManager* schemaMan,
                                                      stats::Stats* stats,
                                                      folly::Executor* executor,
                                                      VertexCache* cache = nullptr) {
        return new QueryBoundWholePushDownV2Processor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::GetNeighborsWholePushDownV2Request& req);

protected:
    explicit QueryBoundWholePushDownV2Processor(kvstore::KVStore* kvstore,
                                                meta::SchemaManager* schemaMan,
                                                stats::Stats* stats,
                                                folly::Executor* executor = nullptr,
                                                VertexCache* cache = nullptr)
        : BaseProcessor<cpp2::GetNeighborsWholePushDownResponse>(kvstore, schemaMan, stats)
        , executor_(executor), vertexCache_(cache) {}

    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId, IndexID idx);

    void onProcessFinished();
private:
    std::vector<BucketWithIndex> genBuckets();
    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(BucketWithIndex bucket);
    cpp2::ErrorCode buildContexts(const cpp2::GetNeighborsWholePushDownV2Request& req);
protected:
    GraphSpaceID  spaceId_;
    std::unique_ptr<ExpressionContext> expCtx_;

    std::unordered_map<TagID, std::shared_ptr<const nebula::meta::SchemaProviderIf>> src_tag_schema_;
    std::unordered_map<EdgeType, std::shared_ptr<const nebula::meta::SchemaProviderIf>> edge_schema_;
    std::unordered_map<std::string, EdgeType> edgeMap_;
    std::unordered_map<std::string, TagID> srcTagMap_;

    std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>> inputs_;
    std::unique_ptr<nebula::meta::SchemaProviderIf> input_schema_;
    int32_t vidIndex_{-1};
    std::vector<EdgeType> edges_;
    std::unique_ptr<Expression> filter_;
    std::vector<nebula::storage::cpp2::YieldColumn> yields_;
    std::vector<std::unique_ptr<nebula::Expression>> yields_expr_;
    std::unordered_map<PartitionID, std::vector<std::vector<nebula::graph::cpp2::RowValue>>> go_result_;

    std::vector<nebula::storage::cpp2::Sentence> sub_sentences_;

    folly::Executor* executor_{nullptr};
    VertexCache* vertexCache_{nullptr};
};

}  // namespace storage
}  // namespace nebula
