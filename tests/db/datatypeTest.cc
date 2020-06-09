////
// @file datatypeTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/datatype.h>
using namespace db;

TEST_CASE("db/datatype.h")
{
    SECTION("CHAR")
    {
        DataType *dt = findDataType("CHAR");
        REQUIRE(dt);

        REQUIRE(dt->size == 65535);
        const char *hello = "hello";
        const char *hello2 = "hello2";
        REQUIRE(dt->compare(hello, hello2, strlen(hello), strlen(hello2)));

        char buffer[32];
        REQUIRE(dt->copy(buffer, hello, 32, strlen(hello) + 1));
        REQUIRE(strncmp(hello, buffer, strlen(hello)) == 0);
    }
    SECTION("BIGINT")
    {
        DataType *dt = findDataType("BIGINT");
        REQUIRE(dt);

        REQUIRE(dt->size == 8);
        long long test1=1;
        long long test2=2;
        REQUIRE(dt->compare(&test1, &test2, 1, 1));
    }
}