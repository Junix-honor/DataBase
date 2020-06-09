////
// @file datatype.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <algorithm>
#include <db/datatype.h>

namespace db {

static bool compareChar(const void *x, const void *y, size_t sx, size_t sy)
{
    return strncmp(
               (const char *) x, (const char *) y, std::max<size_t>(sx, sy)) <
           0;
}
static bool copyChar(void *x, const void *y, size_t sx, size_t sy)
{
    if (sx < sy) return false;
    ::memcpy(x, y, sy);
    return true;
}
static bool compareInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(int *) x < *(int *) y;
}
static bool compareBigInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(long long *) x < *(long long *) y;
}
static bool compareSmallInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(short *) x < *(short *) y;
}
static bool compareTinyInt(const void *x, const void *y, size_t sx, size_t sy)
{
    return *(char *) x < *(char *) y;
}
static bool copyInt(void *x, const void *y, size_t sx, size_t sy)
{
    ::memcpy(x, y, sy);
    return true;
}

DataType *findDataType(const char *name)
{
    static DataType gdatatype[] = {
        {"CHAR", 65535, compareChar, copyChar},     // 0
        {"VARCHAR", -65535, compareChar, copyChar}, // 1
        {"TINYINT", 1, compareTinyInt, copyInt},    // 2
        {"SMALLINT", 2, compareSmallInt, copyInt},  // 3
        {"INT", 4, compareInt, copyInt},            // 4
        {"BIGINT", 8, compareBigInt, copyInt},      // 5
        {},                                         // x
    };

    int index = 0;
    do {
        if (gdatatype[index].name == NULL)
            break;
        else if (strcmp(gdatatype[index].name, name) == 0)
            return &gdatatype[index];
        else
            ++index;
    } while (true);
    return NULL;
}

} // namespace db