////
// @file block.h
// @brief
// 定义block
// block是记录、索引的存储单元。在MySQL和HBase中，存储单元与分配单位是分开的，一般来说，
// 最小分配单元要比block大得多。
// block的布局如下，每个slot占用2B，这要求block最大为64KB。由于记录和索引要求按照4B对
// 齐，BLOCK_DATA、BLOCK_TRAILER也要求4B对齐。
//
// +--------------------+
// |   common header    |
// +--------------------+
// |  data/index header |
// +--------------------+ <--- BLOCK_DATA
// |                    |
// |     data/index     |
// |                    |
// +--------------------+ <--- BLOCK_FREE
// |     free space     |
// +--------------------+
// |       slots        |
// +--------------------+ <--- BLOCK_TRAILER
// |      trailer       |
// +--------------------+
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __OCF_DB_BLOCK_H__
#define __OCF_DB_BLOCK_H__

#include "./record.h"
#include "./checksum.h"

namespace db {

// 通用block头部
class Block : public Field
{
  public:
    // 公共头部字段偏移量
    static const int BLOCK_SIZE = 1024 * 16;         // block大小为16KB
    static const int BLOCK_SPACEID = 0;              // 表空间id，4B
    static const int BLOCK_FREE = BLOCK_SPACEID + 4; // free space位置，2B
    static const int BLOCK_GARBAGE = BLOCK_FREE + 2; // 空洞用链表串联，2B
    static const int BLOCK_NUMBER =
        BLOCK_GARBAGE + 2; // block在表空间的编号，8B
    static const int BLOCK_NEXT = BLOCK_NUMBER + 8; // 下一个block位置，8B
    static const int BLOCK_CHECKSUM_SIZE = 4;       // checksum大小4B
    static const int BLOCK_CHECKSUM =
        BLOCK_SIZE - BLOCK_CHECKSUM_SIZE; // trailer位置

  public:
    // 校验和检验
    inline bool checksum()
    {
        unsigned int sum = db::checksum32(
            (const unsigned char *) this->buffer_, BLOCK_SIZE);
        return !sum;
    }

    // 各字段的位置函数
    inline unsigned int *tableid()
    {
        return (unsigned int *) (this->buffer_ + BLOCK_SPACEID);
    }
    inline unsigned short *freespace()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_FREE);
    }
    inline unsigned short *garbage()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_GARBAGE);
    }
    inline unsigned short *id()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_NUMBER);
    }
    inline unsigned long *next()
    {
        return (unsigned long *) (this->buffer_ + BLOCK_NEXT);
    }
    inline void *trailer() { return (void *) (this->buffer_ + BLOCK_CHECKSUM); }
};

// 数据block
class DataBlock : public Block
{
  public:
    // 数据block各字段偏移量
    static const int BLOCK_DATA_ROWS = BLOCK_NEXT + 8; // 记录个数，2B
    static const int BLOCK_DATA_PADDING =
        BLOCK_DATA_ROWS + 2; // data头部填充，2B
                             // TODO: 指向btree内部节点？
    static const int BLOCK_DATA_START = BLOCK_DATA_PADDING + 2; // 记录开始位置

  public:
    // 各字段位置
    inline unsigned short *rows()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_DATA_ROWS);
    }
    inline char *data() { return (char *) (this->buffer_ + BLOCK_DATA_START); }
    inline unsigned short *slots()
    {
        return (
            unsigned short *) (this->buffer_ + BLOCK_CHECKSUM - 2 * (*rows()));
    }
    // 空闲空间大小
    inline int freesize()
    {
        size_t size = (char *) slots() - data();
        return size / 4 * 4; // slots按2B对齐，可能需要在slots前填充2B
    }
};

} // namespace db

#endif // __OCF_DB_BLOCK_H__
