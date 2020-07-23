/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"


namespace nebula {
namespace graph {

class SampleTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TraverseTestBase::TearDown();
    }
};

TEST_F(SampleTest, OneStep) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SAMPLE FROM %ld OVER serve YIELD serve._dst ORDER BY serve._dst LIMIT 10 ";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SAMPLE FROM %ld OVER serve ORDER BY serve._dst LIMIT 10 ";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"serve._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SAMPLE FROM %ld OVER serve YIELD serve._dst as id ORDER BY $-.id LIMIT 10 ";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {teams_["Spurs"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

}

TEST_F(SampleTest, InvaildCommand) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "SAMPLE FROM %ld OVER serve YIELD serve._dst ";
        auto query = folly::stringPrintf(fmt, players_["Tim Duncan"].vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR , code);
    }
}

TEST_F(SampleTest, TwoStepSample) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "Go FROM %ld OVER like YIELD like._dst as id "
            "| SAMPLE  FROM $-.id OVER like YIELD  like._dst  ORDER BY like.likeness * 3 LIMIT 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["Tony Parker"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "SAMPLE FROM %ld OVER like YIELD like._dst as id  ORDER BY like.likeness LIMIT 1"
            "| GO  FROM $-.id OVER like YIELD  like._dst as id";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
            {players_["LaMarcus Aldridge"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "SAMPLE FROM %ld OVER like YIELD like._dst as id ORDER BY like.likeness LIMIT 1 "
            "| SAMPLE  FROM $-.id OVER like YIELD like._dst as id ORDER BY like.likeness * (1.0 + rand32(0,10)/1000.0) LIMIT 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"id"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {players_["Tim Duncan"].vid()},
            {players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "SAMPLE FROM %ld OVER like YIELD like._dst as id ORDER BY rand32() LIMIT 2 "
            "| SAMPLE  FROM $-.id OVER like YIELD like._dst as id , like.likeness * 10.0 as val ORDER BY $-.val LIMIT 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"id"},  {"val"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, double>> expected = {
            {players_["Tim Duncan"].vid(), 950},
            {players_["Manu Ginobili"].vid(), 950},
            {players_["Tony Parker"].vid(), 950},
            {players_["Manu Ginobili"].vid(), 950},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }


}

TEST_F(SampleTest, fetchInputProp) {
    {
        cpp2::ExecutionResponse resp;
        auto &player = players_["Boris Diaw"];
        auto *fmt =
            "SAMPLE FROM %ld OVER like YIELD like._src as src_id, like._dst as id ORDER BY rand32() LIMIT 2 "
            "| SAMPLE  FROM $-.id OVER like YIELD $-.src_id as src , like._src as hop , like._dst as dst  ORDER BY  like.likeness * 10.0 LIMIT 2";
        auto query = folly::stringPrintf(fmt, player.vid());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"src"},  {"hop"}, {"dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t>> expected = {
            {players_["Boris Diaw"].vid(), players_["Tony Parker"].vid(), players_["Tim Duncan"].vid() },
            {players_["Boris Diaw"].vid(), players_["Tony Parker"].vid(), players_["Manu Ginobili"].vid()},
            {players_["Boris Diaw"].vid(), players_["Tim Duncan"].vid(), players_["Tony Parker"].vid()},
            {players_["Boris Diaw"].vid(), players_["Tim Duncan"].vid(), players_["Manu Ginobili"].vid()},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}


} //graph
} //nebula