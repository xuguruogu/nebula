/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {
class SampleExecutor final : public TraverseExecutor {
public:
    SampleExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char *name() const override {
        return "SampleExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareFrom();

    Status prepareOver();

    Status prepareOverAll();

    Status prepareSampleFilter();

    Status prepareYield();

    Status prepareNeededProps();

    Status checkNeededProps();

    Status addToEdgeTypes(EdgeType type);

    void addStart(VertexID vid);
    /**
    * To obtain the source ids from various places,
    * such as the literal id list, inputs from the pipeline or results of variable.
    */
    Status setupStarts();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsSampleResponse>;
    /**
     * Callback invoked when the stepping out action reaches the dead end.
     */
    void onEmptyInputs();

    void finishExecution(RpcResponse &&rpcResp);


    SampleSentence                              *sentence_{nullptr};
    FromType                                    fromType_{kInstantExpr};
    OverClause::Direction                       direction_{OverClause::Direction::kForward};
    std::vector<EdgeType>                       edgeTypes_;
    std::string                                *varname_{nullptr};
    std::string                                *colname_{nullptr};
    std::vector<nebula::YieldColumn*>           yields_;
    std::unique_ptr<YieldClauseWrapper>         yieldClauseWrapper_;
    storage::cpp2::SampleFilter                 sampleFilter_;
    std::vector<std::string>                    result_columns_;

    using InterimIndex = InterimResult::InterimResultIndex;
    std::shared_ptr<const meta::SchemaProviderIf> scheme_;
    std::unique_ptr<InterimIndex>               index_;
    std::unique_ptr<ExpressionContext>          expCtx_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
    std::vector<graph::cpp2::RowValue>          starts_;
    int                                         vid_idx_{-1};


};
}   // namespace graph
}   // na
