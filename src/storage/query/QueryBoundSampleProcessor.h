/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBOUNDSAMPLEPROCESSOR_H_
#define STORAGE_QUERY_QUERYBOUNDSAMPLEPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

struct Node {
    double score = 0;
    nebula::graph::cpp2::RowValue row;
    friend bool operator<(const Node& n1, const Node& n2) {
        return n1.score > n2.score;
    }
};

class QueryBoundSampleProcessor
    : public BaseProcessor<cpp2::GetNeighborsSampleResponse> {
    FRIEND_TEST(QueryBoundTest, GenBucketsTest);

    struct BucketWithIndex {
        std::vector<std::tuple<PartitionID, VertexID, IndexID>> vertices_;
    };

public:
    static QueryBoundSampleProcessor* instance(kvstore::KVStore* kvstore,
                                               meta::SchemaManager* schemaMan,
                                               stats::Stats* stats,
                                               folly::Executor* executor,
                                               VertexCache* cache = nullptr) {
        return new QueryBoundSampleProcessor(kvstore, schemaMan, stats, executor, cache);
    }

    void process(const cpp2::GetNeighborsSampleRequest& req);

protected:
    explicit QueryBoundSampleProcessor(kvstore::KVStore* kvstore,
                                       meta::SchemaManager* schemaMan,
                                       stats::Stats* stats,
                                       folly::Executor* executor,
                                       VertexCache* cache)
        : BaseProcessor< cpp2::GetNeighborsSampleResponse>(kvstore,schemaMan,stats)
        , executor_(executor), vertexCache_(cache) {}


    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId, IndexID idx);
    void onProcessFinished();

private:
    bool checkExp(const Expression* exp);
    std::vector<BucketWithIndex> genBuckets();
    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(BucketWithIndex bucket);
    cpp2::ErrorCode buildContexts(const cpp2::GetNeighborsSampleRequest& req);
    int32_t getBucketsNum(int32_t verticesNum, int32_t minVerticesPerBucket,int32_t handlerNum);

    std::unique_ptr<nebula::Expression> orderBy_;
    int64_t limitSize_{0};

    GraphSpaceID spaceId_{0};
    std::unique_ptr<ExpressionContext> expCtx_;
    std::unordered_map<TagID, std::shared_ptr<const nebula::meta::SchemaProviderIf>>
        src_tag_schema_;
    std::unordered_map<EdgeType, std::shared_ptr<const nebula::meta::SchemaProviderIf>>
        edge_schema_;
    std::unordered_map<std::string, EdgeType> edgeMap_;
    std::unordered_map<std::string, TagID> srcTagMap_;
    std::unordered_map<PartitionID, std::vector<nebula::graph::cpp2::RowValue>> inputs_;
    std::unique_ptr<nebula::meta::SchemaProviderIf> input_schema_;
    int32_t vidIndex_{-1};
    std::vector<EdgeType> edges_;
    std::unique_ptr<Expression> filter_;
    std::vector<nebula::storage::cpp2::YieldColumn> yields_;
    std::vector<std::unique_ptr<nebula::Expression>> yields_expr_;
    std::unordered_map<PartitionID, std::vector<std::vector<nebula::graph::cpp2::RowValue>>> sample_result_;

    folly::Executor* executor_{nullptr};
    VertexCache* vertexCache_{nullptr};

};

}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_QUERY_QUERYBOUNDSAMPLEPROCESSOR_H_
