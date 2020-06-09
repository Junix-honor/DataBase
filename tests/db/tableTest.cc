////
// @file tableTest.cc
// @brief
// 测试存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/table.h>
using namespace db;

TEST_CASE("db/table.h")
{
    SECTION("create")
    {
        Table table;
        int ret = dbInitialize();
        REQUIRE(ret == S_OK);
        // 填充关系
        RelationInfo relation;
        relation.path = "tablee.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        field.fieldType = "BIGINT";
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        field.fieldType = "CHAR";
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        field.fieldType = "VARCHAR";
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        ret = table.create("tablee", relation);
        REQUIRE(ret == S_OK);
    }
    SECTION("open")
    {
        Table table;
        int ret = table.open("tablee");
        table.initial();
        REQUIRE(ret == S_OK);
        ret = table.openNextBlock();
        REQUIRE(ret == S_OK);
        REQUIRE(table.blockid() == 2);
        table.close("tablee.dat");
    }
    SECTION("insert")
    {
        Table table;
        int ret = table.open("tablee");

        struct iovec iov[3];
        long long id = 3;
        iov[0].iov_base = &id;
        iov[0].iov_len = sizeof(long long);
        char *phone = "13534500702";
        iov[1].iov_base = (void *) phone;
        iov[1].iov_len = strlen(phone) + 1;
        char *name = "Junix";
        iov[2].iov_base = (void *) name;
        iov[2].iov_len = strlen(name) + 1;
        unsigned char header = 0x84;
        ret = table.insert(&header, iov, 3);
        REQUIRE(ret == S_OK);

        id = 1;
        iov[0].iov_base = &id;
        iov[0].iov_len = sizeof(long long);
        phone = "19983485155";
        iov[1].iov_base = (void *) phone;
        iov[1].iov_len = strlen(phone) + 1;
        name = "Honor";
        iov[2].iov_base = (void *) name;
        iov[2].iov_len = strlen(name) + 1;
        header = 0x84;
        ret = table.insert(&header, iov, 3);
        REQUIRE(ret == S_OK);

        Table::iterator it = table.begin();
        Record record = *it;
        iovec keyField;
        record.specialRef(keyField, 0);
        long long *keyFieldPointer = (long long *) keyField.iov_base;
        REQUIRE(*keyFieldPointer == 1);

        table.close("tablee.dat");
    }
    SECTION("iterator")
    {
        Table table;
        int ret = table.open("tablee");
        table.initial();
        REQUIRE(ret == S_OK);
        /* Table::iterator it1 = table.begin();
        REQUIRE(it1 != table.end());
        REQUIRE(++it1 == table.end());
        Table::iterator it2 = table.begin();
        REQUIRE(it2++ == table.begin());
        REQUIRE(it2 == table.end()); */

        Table::iterator it = table.begin();
        Record record = *it;
        REQUIRE(record.fields() == 3);
        table.close("tablee.dat");
    }
    SECTION("remove")
    {
        iovec field;
        long long id = 1;
        field.iov_base = &id;
        field.iov_len = sizeof(long long);

        Table table;
        int ret = table.open("tablee");
        ret = table.remove(field);
        REQUIRE(ret == S_OK);

        Table::iterator it = table.begin();
        Record record = *it;
        iovec Field;
        record.specialRef(Field, 0);
        long long *keyFieldPointer = (long long *) Field.iov_base;
        REQUIRE(*keyFieldPointer == 3);

        record.specialRef(Field, 1);
        char *FieldPointer = (char *) Field.iov_base;
        char *phone = "13534500702";
        REQUIRE(strncmp(FieldPointer, phone, strlen(FieldPointer)) == 0);
        table.close("tablee.dat");
    }
    SECTION("updata")
    {
        iovec field;
        long long keyid = 3;
        field.iov_base = &keyid;
        field.iov_len = sizeof(long long);

        struct iovec iov[3];
        long long id = 3;
        iov[0].iov_base = &id;
        iov[0].iov_len = sizeof(long long);
        char *phone = "13318181238";
        iov[1].iov_base = (void *) phone;
        iov[1].iov_len = strlen(phone) + 1;
        char *name = "Junix";
        iov[2].iov_base = (void *) name;
        iov[2].iov_len = strlen(name) + 1;
        unsigned char header = 0x84;

        Table table;
        int ret = table.open("tablee");
        ret = table.update(field,&header,iov,3);

        Table::iterator it = table.begin();
        Record record = *it;
        iovec Field;
        record.specialRef(Field, 0);
        long long *keyFieldPointer = (long long *) Field.iov_base;
        REQUIRE(*keyFieldPointer == 3);

        record.specialRef(Field, 1);
        char *FieldPointer = (char *) Field.iov_base;
        REQUIRE(strncmp(FieldPointer, phone, strlen(FieldPointer)) == 0);
        table.close("tablee.dat");
    }
    SECTION("destroy")
    {
        Table table;
        int ret = table.open("tablee");
        REQUIRE(ret == S_OK);
        table.close("tablee.dat");
        REQUIRE(table.destroy("tablee.dat") == S_OK);
        REQUIRE(gschema.destroy() == S_OK);
    }
}