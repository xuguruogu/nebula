/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/UseExecutor.h"

DEFINE_bool(use_executor_access_cache, false, "use sentence cache.");

namespace nebula {
namespace graph {

UseExecutor::UseExecutor(Sentence *sentence, ExecutionContext *ectx)
    : Executor(ectx, "use") {
    sentence_ = static_cast<UseSentence*>(sentence);
}


Status UseExecutor::prepare() {
    return Status::OK();
}

void UseExecutor::executeRemote() {
    auto future = ectx()->getMetaClient()->getSpace(*sentence_->space());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(Status::Error("Space not found for `%s'", sentence_->space()->c_str()));
            return;
        }

        auto spaceId = resp.value().get_space_id();

        /**
        * Permission check.
        */
        auto *session = ectx()->rctx()->session();
        auto rst = permission::PermissionManager::canReadSpace(session, spaceId);
        if (!rst) {
            doError(Status::PermissionError("Permission denied"));
            return;
        }

        ectx()->rctx()->session()->setSpace(*sentence_->space(), spaceId);

        FVLOG1("Graph space switched to `%s', space id: %d",
               sentence_->space()->c_str(), spaceId);

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Use space exception: %s", e.what().c_str()));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void UseExecutor::execute() {
    if (!FLAGS_use_executor_access_cache) {
        return executeRemote();
    }

    auto status = ectx()->schemaManager()->toGraphSpaceID(*sentence_->space());
    if (!status.ok()) {
        return executeRemote();
    }

    auto spaceId = std::move(status).value();
    /**
    * Permission check.
    */
    auto *session = ectx()->rctx()->session();
    auto rst = permission::PermissionManager::canReadSpace(session, spaceId);
    if (!rst) {
        doError(Status::PermissionError("Permission denied"));
        return;
    }

    ectx()->rctx()->session()->setSpace(*sentence_->space(), spaceId);

    FVLOG1("Graph space switched to `%s', space id: %d",
           sentence_->space()->c_str(), spaceId);

    doFinish(Executor::ProcessControl::kNext);
}

}   // namespace graph
}   // namespace nebula
