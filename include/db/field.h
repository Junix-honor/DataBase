////
// @file field.h
// @brief
// 结构设置函数
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_FIELD_H__
#define __DB_FIELD_H__

#include <string.h>

namespace db {

// 每一种协议都有自己的消息格式，Protocol类处理消息的填充
class Field
{
  public:
    char *buffer_;  // 消息buffer
    size_t length_; // 消息长度
    size_t offset_; // 处理字段偏移量

  public:
    Field()
        : buffer_(nullptr)
        , length_(0)
        , offset_(0)
    {}

    // 挂上buffer
    inline void attach(char *buf, size_t len)
    {
        buffer_ = buf;
        length_ = len;
        offset_ = 0;
    }
    // 在buffer上追加数据
    bool append(const char *field, size_t len);
    // 从buffer上顺序获取数据
    bool retrieve(void *field, size_t len);

    inline size_t left() { return length_ - offset_; }
    inline bool is_end() { return length_ == offset_; }
};

} // namespace db

#endif // __DB_FIELD_H__
