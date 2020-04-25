/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "storage/client/StorageClient.h"
#include "GoExecutor.h"

namespace nebula {

namespace storage {
namespace cpp2 {
class QueryResponse;
}   // namespace cpp2
}   // namespace storage

namespace graph {

class GoWholePushDownV2Executor final : public TraverseExecutor {
public:
    GoWholePushDownV2Executor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GoWholePushDownV2Executor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    Status prepareDistinct();

    Status prepareOverAll();

    Status prepareSubSentences();

    Status checkNeededProps();

    /**
     * To obtain the source ids from various places,
     * such as the literal id list, inputs from the pipeline or results of variable.
     */
    Status setupStarts();

    /**
     * To step out for one step.
     */
    void stepOut();

    using RpcResponse = storage::StorageRpcResponse<storage::cpp2::GetNeighborsWholePushDownResponse>;
    /**
     * Callback invoked upon the response of stepping out arrives.
     */
    void onStepOutResponse(RpcResponse &&rpcResp);

    /**
     * Callback invoked when the stepping out action reaches the dead end.
     */
    void onEmptyInputs();

    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };

    void addStart(VertexID vid);
    void addStart(graph::cpp2::RowValue row);

private:
  GoSentence                                 *sentence_{nullptr};
  FromType                                    fromType_{kInstantExpr};
  std::vector<EdgeType>                       edgeTypes_;
  std::string                                *varname_{nullptr};
  std::string                                *colname_{nullptr};
  std::vector<YieldColumn*>                   yields_;
  bool                                        distinct_{false};
  std::unique_ptr<YieldClauseWrapper>         yieldClauseWrapper_;
  std::unique_ptr<ExpressionContext>          expCtx_;
  std::unique_ptr<cpp2::ExecutionResponse>    resp_;

  std::shared_ptr<const meta::SchemaProviderIf> scheme_;
  std::vector<graph::cpp2::RowValue>          starts_;
  int                                         vid_idx_{-1};
  std::vector<std::string>                    result_columns_;
  std::vector<storage::cpp2::Sentence>        sentencesPushDown_;
};

}   // namespace graph
}   // namespace nebula

