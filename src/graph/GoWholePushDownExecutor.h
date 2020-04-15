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

class GoWholePushDownExecutor final : public TraverseExecutor {
public:
    GoWholePushDownExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GoWholePushDownExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    /**
     * To do some preparing works on the clauses
     */
    Status prepareClauses();

    Status prepareStep();

    Status prepareFrom();

    Status prepareOver();

    Status prepareWhere();

    Status prepareYield();

    Status prepareNeededProps();

    Status prepareDistinct();

    Status prepareOverAll();

    Status prepareOrderBy();

    Status prepareLimit();

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

    void fetchVertexProps(std::vector<VertexID> ids, RpcResponse &&rpcResp);

    /**
     * To retrieve or generate the column names for the execution result.
     */
    std::vector<std::string> getResultColumnNames() const;

    /**
     * get the edgeName when over all edges
     */
    std::vector<std::string> getEdgeNames() const;

    /**
     * To setup an intermediate representation of the execution result,
     * which is about to be piped to the next executor.
     */
    bool setupInterimResult(const std::vector<cpp2::RowValue>& rows, std::unique_ptr<InterimResult> &result);

    /**
     * To setup the header of the execution result, i.e. the column names.
     */
    void setupResponseHeader(cpp2::ExecutionResponse &resp) const;

    /**
     * To setup the body of the execution result.
     */
    bool setupResponseBody(RpcResponse &rpcResp, cpp2::ExecutionResponse &resp) const;

    /**
     * To iterate on the final data collection, and evaluate the filter and yield columns.
     * For each row that matches the filter, `cb' would be invoked.
     */
    using Callback = std::function<Status(std::vector<VariantType>,
                                          const std::vector<nebula::cpp2::SupportedType>&)>;

    bool processFinalResult(RpcResponse &rpcResp, Callback cb) const;

    /**
     * A container to hold the mapping from vertex id to its properties, used for lookups
     * during the final evaluation process.
     */
    class VertexHolder final {
    public:
        OptVariantType getDefaultProp(TagID tid, const std::string &prop) const;
        OptVariantType get(VertexID id, TagID tid, const std::string &prop) const;
        void add(const storage::cpp2::QueryResponse &resp);
        nebula::cpp2::SupportedType getDefaultPropType(TagID tid, const std::string &prop) const;
        nebula::cpp2::SupportedType getType(VertexID id, TagID tid, const std::string &prop);

    private:
        using VData = std::tuple<std::shared_ptr<ResultSchemaProvider>, std::string>;
        std::unordered_map<VertexID, std::unordered_map<TagID, VData>> data_;
    };

    OptVariantType getPropFromInterim(VertexID id, const std::string &prop) const;

    enum FromType {
        kInstantExpr,
        kVariable,
        kPipe,
    };

private:
  GoSentence                                 *sentence_{nullptr};
  FromType                                    fromType_{kInstantExpr};
  std::vector<EdgeType>                       edgeTypes_;
  std::string                                *varname_{nullptr};
  std::string                                *colname_{nullptr};
  std::unique_ptr<WhereWholePushDownWrapper>  whereWrapper_;
  std::vector<YieldColumn*>                   yields_;
  std::unique_ptr<YieldClauseWrapper>         yieldClauseWrapper_;
  bool                                        distinct_{false};
  bool                                        distinctPushDown_{false};
  using InterimIndex = InterimResult::InterimResultIndex;
  std::unique_ptr<InterimIndex>               index_;
  std::unique_ptr<ExpressionContext>          expCtx_;
  std::vector<VertexID>                       starts_;
  std::unique_ptr<VertexHolder>               vertexHolder_;
  std::unique_ptr<cpp2::ExecutionResponse>    resp_;
  const InterimResult*                        p_inputs_{nullptr};
  // The name of Tag or Edge, index of prop in data
  using SchemaPropIndex = std::unordered_map<std::pair<std::string, std::string>, int64_t>;
//  std::vector<std::pair<std::string, OrderFactor::OrderType>> layerOrderBySortFactors_;
  std::vector<nebula::storage::cpp2::OrderByFactor>           layerOrderBySortFactors_;
  int64_t                                                     layerLimitCount_{-1};
  int64_t                                                     layerLimitOffset_{-1};
  int64_t                                                     layerLimitCountPushDown_{-1};
};

}   // namespace graph
}   // namespace nebula

