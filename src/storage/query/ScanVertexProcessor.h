/* Copyright (c) 2019 vesoft inc. All rights reserved.:
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_SCANVERTEXPROCESSOR_H_
#define STORAGE_SCANVERTEXPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ScanVertexProcessor
    : public BaseProcessor<cpp2::ScanVertexResponse> {
public:
    static ScanVertexProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         stats::Stats* stats,
                                         folly::Executor* executor = nullptr) {
        return new ScanVertexProcessor(kvstore, schemaMan, stats, executor);
    }

    void process(const cpp2::ScanVertexRequest& req);

private:
    explicit ScanVertexProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 stats::Stats* stats,
                                 folly::Executor* executor = nullptr)
        : BaseProcessor<cpp2::ScanVertexResponse>(kvstore, schemaMan, stats)
        , executor_(executor) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanVertexRequest& req);

    std::unordered_map<TagID, std::vector<PropContext>> tagContexts_;
    std::unordered_map<TagID, nebula::cpp2::Schema> tagSchema_;
    bool returnAllColumns_{false};
    GraphSpaceID spaceId_;
    PartitionID partId_;
    int64_t rowLimit_{0};
    int64_t startTime_{0};
    int64_t endTime_{0};
    std::string start_;
    std::string prefix_;

    folly::Executor* executor_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_SCANVERTEXPROCESSOR_H_
