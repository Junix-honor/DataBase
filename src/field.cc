////
// @file field.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/field.h>

namespace db {

// 在buffer上追加数据
bool Field::append(const char *field, size_t len)
{
    if (offset_ + len <= length_) {
        memcpy(buffer_ + offset_, field, len);
        offset_ += len;
        return true;
    } else
        return false;
}
// 从buffer上顺序获取数据
bool Field::retrieve(void *field, size_t len)
{
    if (offset_ + len <= length_) {
        memcpy(field, buffer_ + offset_, len);
        offset_ += len;
        return true;
    } else
        return false;
}

} // namespace db