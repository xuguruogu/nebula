/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/client/StorageClient.h"
#include "common/time/Duration.h"
#include "common/utils/to_string.h"

DEFINE_int32(storage_client_timeout_ms, 60 * 1000, "storage client timeout");

namespace nebula {
namespace storage {

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::AddVerticesRequest& req) {
    return client.future_addVertices(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::AddEdgesRequest& req) {
    return client.future_addEdges(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::GetNeighborsSampleRequest& req) {
    return client.future_getBoundSample(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::VertexPropRequest& req) {
    return client.future_getProps(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::EdgePropRequest& req) {
    return client.future_getEdgeProps(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::GetNeighborsRequest& req) {
    return client.future_getBound(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::DeleteEdgesRequest& req) {
    return client.future_deleteEdges(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::DeleteVerticesRequest& req) {
    return client.future_deleteVertices(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::PutRequest& req) {
    return client.future_put(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::GetRequest& req) {
    return client.future_get(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::LookUpIndexRequest& req) {
    return client.future_lookUpIndex(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::UpdateVertexRequest& req) {
    return client.future_updateVertex(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::UpdateEdgeRequest& req) {
    return client.future_updateEdge(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::GetUUIDReq& req) {
    return client.future_getUUID(req);
}

static auto
processOne(storage::cpp2::StorageServiceAsyncClient& client, const cpp2::ScanVertexRequest& req) {
    return client.future_scanVertex(req);
}

template <
    class Request,
    class Response = typename decltype(processOne(
        *(storage::cpp2::StorageServiceAsyncClient*)(nullptr),
        *(const Request*)(nullptr))
    )::value_type
>
struct StorageReqsContext : public std::enable_shared_from_this<StorageReqsContext<Request, Response>> {
public:
    StorageReqsContext(
        StorageClient& client,
        std::unordered_map<HostAddr, Request> requests,
        folly::EventBase* evb) :
        client_(client),
        requests_(std::move(requests)),
        resp_(requests_.size()),
        evb_(evb == nullptr ? client.ioThreadPool_->getEventBase() : evb) {
    }

    folly::SemiFuture<StorageRpcResponse<Response>> process(bool retry = false) {
        std::vector<folly::Future<folly::Unit>> results;
        for (auto& pair : requests_) {
            const HostAddr& host = pair.first;
            Request& req = pair.second;

            results.emplace_back(folly::via(evb_, [this, &host, &req] () mutable {
                return processOne(*getClient(host), req).via(evb_);
            }).thenTry([this, &host, &req, retry] (folly::Try<Response>&& reqTry) {
                if (reqTry.hasException()) {
                    LOG(ERROR) << "Request to " << host << " failed: " << reqTry.exception().what();
                    if (!retry) {
                        for (auto& part : req.parts) {
                            PartitionID partId = getPartId(part);
                            resp_.failedParts().emplace(partId, storage::cpp2::ErrorCode::E_RPC_FAILURE);
                            client_.invalidLeader(req.get_space_id(), partId);
                        }
                        resp_.markFailure();
                    } else {
                        requestsRetry_[host] = std::move(req);
                    }
                } else {
                    auto resp = std::move(reqTry).value();
                    auto& result = resp.get_result();
                    for (auto& code : result.get_failed_codes()) {
                        VLOG(3) << "Failure! Failed part " << code.get_part_id()
                                << ", failed code " << static_cast<int32_t>(code.get_code());
                        if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                            auto* leader = code.get_leader();
                            if (leader != nullptr
                                && leader->get_ip() != 0
                                && leader->get_port() != 0) {
                                client_.updateLeader(req.get_space_id(),
                                                     code.get_part_id(),
                                                     HostAddr(leader->get_ip(), leader->get_port()));
                            } else {
                                client_.invalidLeader(req.get_space_id(), code.get_part_id());
                            }
                        } else if (code.get_code() == storage::cpp2::ErrorCode::E_PART_NOT_FOUND
                                   || code.get_code() == storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                            client_.invalidLeader(req.get_space_id(), code.get_part_id());
                        } else {
                            // Simply keep the result
                            resp_.failedParts().emplace(code.get_part_id(), code.get_code());
                        }
                    }
                    if (!result.get_failed_codes().empty()) {
                        resp_.markFailure();
                    }

                    // Adjust the latency
                    auto latency = result.get_latency_in_us();
                    resp_.setLatency(host, latency, duration_.elapsedInUSec());
                    // Keep the response
                    resp_.responses().emplace_back(std::move(resp));
                }
            }));
        }

        return folly::collectAll(results).via(evb_).then([this, retry, s = this->shared_from_this()] (auto&& t) {
            UNUSED(t);
            if (retry && resp_.succeeded() && !requestsRetry_.empty()) {
                requests_.clear();
                std::unordered_set<HostAddr> blackList;
                std::unordered_set<HostAddr> retryList;
                std::unordered_set<PartitionID> partList;
                for (auto& pair : requestsRetry_) {
                    auto& host = pair.first;
                    blackList.insert(host);
                }

                for (auto& pair : requestsRetry_) {
                    auto& req = pair.second;
                    auto spaceId = req.get_space_id();
                    for (auto& part : req.parts) {
                        PartitionID partId = getPartId(part);
                        partList.insert(partId);
                        auto metaStatus = client_.getPartMeta(spaceId, partId);
                        if (!metaStatus.ok()) {
                            LOG(ERROR) << metaStatus.status();
                            return respond();
                        }
                        std::vector<HostAddr> peers = metaStatus.value().peers_;
                        CHECK_GT(peers.size(), 0U);
                        std::random_shuffle(peers.begin(), peers.end());
                        bool find = false;
                        for (auto& host : peers) {
                            if (blackList.find(host) == blackList.end()) {
                                auto iter = requests_.find(host);
                                if (iter == requests_.end()) {
                                    auto r = req;
                                    r.parts.clear();
                                    r.parts.insert(r.parts.end(), part);
                                    requests_[host] = std::move(r);
                                    retryList.insert(host);
                                } else {
                                    auto r = iter->second;
                                    r.parts.insert(r.parts.end(), part);
                                }
                                find = true;
                                break;
                            }
                        }
                        if (!find) {
                            return respond();
                        }
                    }
                }
                requestsRetry_.clear();
                LOG(INFO) << "retry failed request, " << partList << "@hosts: " << blackList << " -> " << retryList;
                return process(false);
            }
            return respond();
        });
    }

    folly::SemiFuture<StorageRpcResponse<Response>> respond() {
        for (auto& pair : requestsRetry_) {
            Request& req = pair.second;
            for (auto& part : req.parts) {
                PartitionID partId = getPartId(part);
                resp_.failedParts().emplace(partId, storage::cpp2::ErrorCode::E_RPC_FAILURE);
                client_.invalidLeader(req.get_space_id(), partId);
            }
            resp_.markFailure();
        }
        return folly::makeSemiFuture(std::move(resp_));
    }

private:
    template <typename T>
    static PartitionID getPartId(const std::pair<const PartitionID, T>& p) {
        return p.first;
    }

    static PartitionID getPartId(const PartitionID& p) {
        return p;
    }

    std::shared_ptr<storage::cpp2::StorageServiceAsyncClient>
    getClient(HostAddr host) {
        return client_.clientsMan_->client(
            host, evb_, false, FLAGS_storage_client_timeout_ms);
    }
private:
    time::Duration duration_;
    StorageClient& client_;
    std::unordered_map<HostAddr, Request> requests_{};
    std::unordered_map<HostAddr, Request> requestsRetry_{};
    StorageRpcResponse<Response> resp_;
    folly::EventBase* evb_{};
};

template <
    class Request,
    class Response = typename decltype(processOne(
        *(storage::cpp2::StorageServiceAsyncClient*)(nullptr),
        *(const Request*)(nullptr))
    )::value_type
>
std::shared_ptr<StorageReqsContext<Request, Response>>
makeStorageReqsContext(StorageClient& client,
                      std::unordered_map<HostAddr, Request>&& requests,
                      folly::EventBase* evb) {
    return std::make_shared<StorageReqsContext<Request, Response>>(
        client, std::forward<std::unordered_map<HostAddr, Request>>(requests), evb);
}

template <
    class Request,
    class Response = typename decltype(processOne(
        *(storage::cpp2::StorageServiceAsyncClient*)(nullptr),
        *(const Request*)(nullptr))
    )::value_type
>
struct StorageReqContext : public std::enable_shared_from_this<StorageReqContext<Request, Response>> {
public:
    StorageReqContext(StorageClient& client, HostAddr host, Request req, folly::EventBase* evb) :
        client_(client), host_(host), req_(std::move(req)),
        evb_(evb == nullptr ? client.ioThreadPool_->getEventBase() : evb) {
    }

    folly::Future<StatusOr<Response>> process(bool retry = false) {
        return folly::via(evb_, [this] () mutable {
            return processOne(*getClient(host_), req_).via(evb_);
        }).thenTry([this, retry, s = this->shared_from_this()] (folly::Try<Response>&& reqTry) {
            auto spaceId = req_.get_space_id();
            auto partId = req_.get_part_id();
            if (reqTry.hasException()) {
                LOG(ERROR) << "Request to " << host_ << " failed: " << reqTry.exception().what();
                if (retry) {
                    auto metaStatus = client_.getPartMeta(spaceId, partId);
                    if (metaStatus.ok()) {
                        std::vector<HostAddr> peers = metaStatus.value().peers_;
                        CHECK_GT(peers.size(), 0U);
                        std::random_shuffle(peers.begin(), peers.end());
                        for (auto& host : peers) {
                            if (host != host_) {
                                LOG(INFO) << "retry failed request, " << partId << "@host: " << host_ << " -> " << host;
                                host_ = host;
                                return process(false);
                            }
                        }
                    }
                }
                stats::Stats::addStatsValue(
                    client_.stats_.get(), false, duration_.elapsedInUSec());
                client_.invalidLeader(spaceId, partId);
                return folly::makeFuture<StatusOr<Response>>(Status::Error(folly::stringPrintf(
                    "RPC failure in StorageClient: %s", reqTry.exception().what().c_str())));
            } else {
                auto resp = std::move(reqTry).value();
                auto& result = resp.get_result();
                for (auto& code : result.get_failed_codes()) {
                    if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                        auto* leader = code.get_leader();
                        if (leader != nullptr && leader->get_ip() != 0 && leader->get_port() != 0) {
                            client_.updateLeader(spaceId, code.get_part_id(),
                                                 HostAddr(leader->get_ip(), leader->get_port()));
                        } else {
                            client_.invalidLeader(spaceId, code.get_part_id());
                        }
                    } else if (code.get_code() == storage::cpp2::ErrorCode::E_PART_NOT_FOUND ||
                               code.get_code() == storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                        client_.invalidLeader(spaceId, code.get_part_id());
                    }
                }
                stats::Stats::addStatsValue(
                    client_.stats_.get(), result.get_failed_codes().empty(),duration_.elapsedInUSec());
                return folly::makeFuture<StatusOr<Response>>(std::move(resp));
            }
        });
    }
private:
    std::shared_ptr<storage::cpp2::StorageServiceAsyncClient>
    getClient(HostAddr host) {
        return client_.clientsMan_->client(
            host, evb_, false, FLAGS_storage_client_timeout_ms);
    }
private:
    time::Duration duration_;
    StorageClient& client_;
    HostAddr host_;
    Request req_;
    folly::EventBase* evb_{};
};

template <
    class Request,
    class Response = typename decltype(processOne(
        *(storage::cpp2::StorageServiceAsyncClient*)(nullptr),
        *(const Request*)(nullptr))
    )::value_type
>
std::shared_ptr<StorageReqContext<Request, Response>>
makeStorageReqContext(StorageClient& client, HostAddr host, Request req, folly::EventBase* evb) {
    return std::make_shared<StorageReqContext<Request, Response>>(
        client, host, std::forward<Request>(req), evb);
}

StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool,
                             meta::MetaClient *client,
                             const std::string &serviceName)
        : ioThreadPool_(threadPool)
        , client_(client) {
    clientsMan_
        = std::make_unique<thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>();
    stats_ = std::make_unique<stats::Stats>(serviceName, "storageClient");
}


StorageClient::~StorageClient() {
    VLOG(3) << "~StorageClient";
    if (nullptr != client_) {
        client_ = nullptr;
    }
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::addVertices(
        GraphSpaceID space,
        std::vector<cpp2::Vertex> vertices,
        bool overwritable,
        folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, vertices, [](const cpp2::Vertex& v) { return v.get_id(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::AddVerticesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_parts(std::move(c.second));
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process();
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::Edge& e) { return e.get_key().get_src(); });
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::AddEdgesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_parts(std::move(c.second));
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process();
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getNeighbors(
        GraphSpaceID space,
        const std::vector<VertexID> &vertices,
        const std::vector<EdgeType> &edgeTypes,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vertices, [](const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::QueryResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_edge_types(edgeTypes);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}

folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsSampleResponse>> StorageClient::sampleNeighbors(
    GraphSpaceID space,
    nebula::cpp2::Schema schema,
    std::vector<graph::cpp2::RowValue> rows,
    int vid_idx,
    const std::vector<EdgeType> &edgeTypes,
    storage::cpp2::SampleFilter sample_filter,
    std::vector<storage::cpp2::YieldColumn> yields,
    folly::EventBase* evb) {

    auto status = clusterIdsToHosts(
        space, std::move(rows), [vid_idx] (const graph::cpp2::RowValue& row) {
            const nebula::graph::cpp2::ColumnValue& col = row.columns.at(vid_idx);
            switch (col.getType()) {
                case nebula::graph::cpp2::ColumnValue::Type::id:
                    return col.get_id();
                case nebula::graph::cpp2::ColumnValue::Type::integer:
                    return col.get_integer();
                default:
                    throw std::runtime_error("sampleNeighbors not supported vid type.");
            }
        });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsSampleResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::GetNeighborsSampleRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_schema(schema);
        req.set_parts(std::move(c.second));
        req.set_vid_index(vid_idx);
        req.set_edge_types(edgeTypes);
        req.set_sample_filter(sample_filter);
        req.set_yields(yields);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}

folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vertices, [](const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::QueryResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::VertexPropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_return_columns(returnCols);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}


folly::SemiFuture<StorageRpcResponse<cpp2::EdgePropResponse>> StorageClient::getEdgeProps(
        GraphSpaceID space,
        std::vector<cpp2::EdgeKey> edges,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::EdgeKey& v) { return v.get_src(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::EdgePropResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::EdgePropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        for (auto& p : c.second) {
            req.set_edge_type((p.second[0].edge_type));
            break;
        }
        req.set_parts(std::move(c.second));
        req.set_return_columns(returnCols);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::deleteEdges(
    GraphSpaceID space,
    std::vector<storage::cpp2::EdgeKey> edges,
    folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::EdgeKey& v) { return v.get_src(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::DeleteEdgesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process();
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::deleteVertices(
    GraphSpaceID space,
    std::vector<VertexID> vids,
    folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vids, [] (const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::DeleteVerticesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process();
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateVertex(
        GraphSpaceID space,
        VertexID vertexId,
        std::string filter,
        std::vector<storage::cpp2::UpdateItem> updateItems,
        std::vector<std::string> returnCols,
        bool insertable,
        folly::EventBase* evb) {

    auto status = partId(space, vertexId);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);
    cpp2::UpdateVertexRequest req;
    req.set_space_id(space);
    req.set_vertex_id(vertexId);
    req.set_part_id(part);
    req.set_filter(filter);
    req.set_update_items(std::move(updateItems));
    req.set_return_columns(returnCols);
    req.set_insertable(insertable);

    return makeStorageReqContext(*this, host, std::move(req), evb)->process();
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateEdge(
        GraphSpaceID space,
        storage::cpp2::EdgeKey edgeKey,
        std::string filter,
        std::vector<storage::cpp2::UpdateItem> updateItems,
        std::vector<std::string> returnCols,
        bool insertable,
        folly::EventBase* evb) {
    auto status = partId(space, edgeKey.get_src());
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(space);
    req.set_edge_key(edgeKey);
    req.set_part_id(part);
    req.set_filter(filter);
    req.set_update_items(std::move(updateItems));
    req.set_return_columns(returnCols);
    req.set_insertable(insertable);

    return makeStorageReqContext(*this, host, std::move(req), evb)->process();
}


folly::Future<StatusOr<cpp2::GetUUIDResp>> StorageClient::getUUID(
        GraphSpaceID space,
        const std::string& name,
        folly::EventBase* evb) {
    std::hash<std::string> hashFunc;
    auto hashValue = hashFunc(name);
    auto status = partId(space, hashValue);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);

    cpp2::GetUUIDReq req;
    req.set_space_id(space);
    req.set_part_id(part);
    req.set_name(name);

    return makeStorageReqContext(*this, host, std::move(req), evb)->process(true);
}

StatusOr<PartitionID> StorageClient::partId(GraphSpaceID spaceId, int64_t id) const {
    auto status = partsNum(spaceId);
    if (!status.ok()) {
        return Status::Error("Space not found, spaceid: %d", spaceId);
    }

    auto parts = status.value();
    auto s = ID_HASH(id, parts);
    CHECK_GE(s, 0U);
    return s;
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
StorageClient::put(GraphSpaceID space,
                   std::vector<nebula::cpp2::Pair> values,
                   folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, values, [](const nebula::cpp2::Pair& v) {
        return std::hash<std::string>{}(v.get_key());
    });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::PutRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process();
}

folly::SemiFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>>
StorageClient::get(GraphSpaceID space,
                   const std::vector<std::string>& keys,
                   bool returnPartly,
                   folly::EventBase* evb) {
    auto status = clusterIdsToHosts(
        space, keys, [](const std::string& v) { return std::hash<std::string>{}(v); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::GetRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_return_partly(returnPartly);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}

folly::SemiFuture<StorageRpcResponse<storage::cpp2::LookUpIndexResp>>
StorageClient::lookUpIndex(GraphSpaceID space,
                           IndexID indexId,
                           std::string filter,
                           std::vector<std::string> returnCols,
                           bool isEdge,
                           folly::EventBase *evb) {
    auto status = getHostParts(space);
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<storage::cpp2::LookUpIndexResp>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::LookUpIndexRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_index_id(indexId);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
        req.set_is_edge(isEdge);
    }

    return makeStorageReqsContext(*this, std::move(requests), evb)->process(true);
}

folly::Future<StatusOr<storage::cpp2::ScanVertexResponse>>
StorageClient::ScanVertex(GraphSpaceID space,
                          PartitionID partId,
                          std::string cursor,
                          std::unordered_map<TagID, std::vector<storage::cpp2::PropDef>> return_columns,
                          bool all_columns,
                          int32_t limit,
                          int64_t start_time,
                          int64_t end_time,
                          folly::EventBase *evb) {
    auto metaStatus = getPartMeta(space, partId);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::ScanVertexResponse>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);
    VLOG(1) << "ScanVertex partId " << partId << " @" << host;
    cpp2::ScanVertexRequest req;
    req.set_space_id(space);
    req.set_part_id(partId);
    req.set_cursor(std::move(cursor));
    req.set_return_columns(std::move(return_columns));
    req.set_all_columns(all_columns);
    req.set_limit(limit);
    req.set_start_time(start_time);
    req.set_end_time(end_time);

    return makeStorageReqContext(*this, host, std::move(req), evb)->process(true);
}

}   // namespace storage
}   // namespace nebula
