/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "fs/TempDir.h"
#include "http/HttpClient.h"
#include "webservice/Router.h"
#include "webservice/WebService.h"
#include "storage/test/TestUtils.h"
#include "storage/http/StorageHttpIngestHandler.h"
#include <gtest/gtest.h>
#include <rocksdb/sst_file_writer.h>

namespace nebula {
namespace storage {

class StorageHttpIngestHandlerTestEnv : public ::testing::Environment {
public:
    void init(std::string path) {
        auto partPath = folly::stringPrintf(
            "%s/disk1/nebula/0/download/%s/0",
            rootPath_->path(), path.c_str());
        ASSERT_TRUE(nebula::fs::FileUtils::makeDir(partPath));

        auto options = rocksdb::Options();
        auto env = rocksdb::EnvOptions();
        rocksdb::SstFileWriter writer{env, options};

        auto sstPath = folly::stringPrintf("%s/data.sst", partPath.c_str());
        auto status = writer.Open(sstPath);
        ASSERT_EQ(rocksdb::Status::OK(), status);

        for (auto i = 0; i < 10; i++) {
            status = writer.Put(folly::stringPrintf("key_%d", i),
                                folly::stringPrintf("val_%d", i));
            ASSERT_EQ(rocksdb::Status::OK(), status);
        }
        status = writer.Finish();
        ASSERT_EQ(rocksdb::Status::OK(), status);

    }
    void SetUp() override {
        FLAGS_ws_http_port = 0;
        FLAGS_ws_h2_port = 0;
        VLOG(1) << "Starting web service...";

        rootPath_ = std::make_unique<fs::TempDir>("/tmp/StorageHttpIngestHandler.XXXXXX");
        kv_ = TestUtils::initKV(rootPath_->path(), 1);

        init("general");
        init("edge/1");
        init("tag/1");

        webSvc_ = std::make_unique<WebService>();
        auto& router = webSvc_->router();
        router.get("/ingest").handler([this](nebula::web::PathParams&&) {
            auto handler = new storage::StorageHttpIngestHandler();
            handler->init(kv_.get());
            return handler;
        });
        auto webStatus = webSvc_->start();
        ASSERT_TRUE(webStatus.ok()) << webStatus;
    }

    void TearDown() override {
        kv_.reset();
        rootPath_.reset();
        webSvc_.reset();
        VLOG(1) << "Web service stopped";
    }

private:
    std::unique_ptr<fs::TempDir> rootPath_;
    std::unique_ptr<kvstore::KVStore> kv_;
    std::unique_ptr<WebService> webSvc_;
};

TEST(StorageHttpIngestHandlerTest, StorageIngestTest) {
    {
        auto url = "/ingest?space=0";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
    {
        auto url = "/ingest?space=0&edge=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
    {
        auto url = "/ingest?space=0&tag=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
    {
        auto url = "/ingest?space=1";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest failed", resp.value());
    }
    {
        auto url = "/ingest?space=0&edge=2";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
    {
        auto url = "/ingest?space=0&tag=2";
        auto request = folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(),
                                           FLAGS_ws_http_port, url);
        auto resp = http::HttpClient::get(request);
        ASSERT_TRUE(resp.ok());
        ASSERT_EQ("SSTFile ingest successfully", resp.value());
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    ::testing::AddGlobalTestEnvironment(new nebula::storage::StorageHttpIngestHandlerTestEnv());

    return RUN_ALL_TESTS();
}
