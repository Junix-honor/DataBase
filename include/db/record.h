////
// @file record.h
// @brief
// 定义数据库记录
// 采用类似MySQL的记录方案，一条记录分为四个部分：
// 记录总长度+字段长度数组+Header+字段
// 1. 记录总长度从最开始算；
// 2. 字段长度是一个逆序变长数组，记录每条记录从Header开始的头部偏移量位置；
// 3. Header存放一些相关信息，1B；
// 4. 然后是各字段顺序摆放；
//
// | T | M | x | x | x | x | x | x |
//   ^   ^
//   |   |
//   |   +-- 最小记录
//   +-- tombstone
//
// 记录的分配按照4B对齐，同时要求block头部至少按照4B对齐
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __OCF_DB_RECORD_H__
#define __OCF_DB_RECORD_H__

#include <utility>
#include "./integer.h"
#include "./field.h"

/* Structure for scatter/gather I/O.  */
struct iovec
{
    void *iov_base; /* Pointer to data.  */
    size_t iov_len; /* Length of data.  */
};

namespace db {

// 物理记录
class Record : public Field
{
  public:
    static const int HEADER_SIZE = 1; // 头部1B
    static const int ALIGN_SIZE = 4;  // 按4B对齐

    static const int BYTE_TOMBSTONE = 1; // tombstone在header的第1字节
    static const unsigned char MASK_TOMBSTONE = 0x80; // tombstone掩码
    static const int BYTE_MIMIMUM = 1; // 最小记录标记在header的第1字节
    static const unsigned char MASK_MINIMUM = 0x40; // 最小记录掩码

  public:
    // 整个记录长度+header偏移量
    std::pair<size_t, size_t> size(const iovec *iov, int iovcnt);

    // 向buffer里写各个域
    bool set(const iovec *iov, int iovcnt, const unsigned char *header);
    // 从buffer获取各字段
    bool get(iovec *iov, int iovcnt, unsigned char *header);
    // TODO:
    void dump(char *buf, size_t len);

    // 获得记录总长度，包含头部+变长偏移数组+长度+记录
    size_t length(size_t offset);
    // 获取记录字段个数
    size_t fields(size_t offset);
};

} // namespace db

#endif // __OCF_DB_RECORD_H__
