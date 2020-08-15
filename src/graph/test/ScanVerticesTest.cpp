/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/test/TraverseTestBase.h"
#include "meta/test/TestUtils.h"


namespace nebula {
namespace graph {

class ScanVerticesTest : public TraverseTestBase {
protected:
    void SetUp() override {
        TraverseTestBase::SetUp();
    }

    void TearDown() override {
        TraverseTestBase::TearDown();
    }
};

TEST_F(ScanVerticesTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute("SCAN VERTEX player LIMIT 1", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"age"}, {"__version__"}, {"__ts__"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute("SCAN VERTEX player PART 1 LIMIT 1", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"age"}, {"__version__"}, {"__ts__"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute("SCAN VERTEX player FROM 0 LIMIT 1", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"age"}, {"__version__"}, {"__ts__"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
    }
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute(
            "yield rand64(0, 1) as p | scan vertex player part ($-.p + 1) from (rand64() / 1 * 1 + $-.p) limit 10", resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}  // namespace graph
}  // namespace nebula
