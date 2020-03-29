////
// @file fieldTest.cc
// @brief
// 测试field
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/field.h>

using namespace db;

TEST_CASE("db/field.h")
{
    SECTION("attach")
    {
        Field field;
        char buf[4096];
        field.attach(buf, 4096);
        REQUIRE(field.buffer_ == buf);
        REQUIRE(field.length_ == 4096);
        REQUIRE(field.offset_ == 0);
    }

    // SECTION("append") {}
}