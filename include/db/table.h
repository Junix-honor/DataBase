////
// @file table.h
// @brief
// 存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__
#include <db/schema.h>
#include <db/block.h>
#include <db/record.h>
#include <string>
#include <utility>
#include <vector>
#include <db/config.h>
#include <algorithm>

namespace db {

////
// @brief
// 表操作接口
//

class Table
{
  public:
    friend struct Compare;

  public:
    // 表的迭代器
    struct iterator
    {
      private:
        unsigned int blockid; // block位置
        unsigned short sloti; // slots[]索引
        Table &table;
        Record record;

      public:
        iterator(unsigned int bid, unsigned short si, Table &itable)
            : blockid(bid)
            , sloti{si}
            , table(itable) //?赋值构造
        {}
        iterator(const iterator &o)
            : blockid(o.blockid)
            , sloti(o.sloti)
            , table(o.table)
        {}
        iterator &operator=(const iterator &o)
        {
            blockid = o.blockid;
            sloti = o.blockid;
            table = o.table;
            return *this;
        }
        iterator &operator++() // 前缀
        {
            DataBlock block;
            block.attach(table.buffer_);
            if (sloti < block.getSlotsNum()) sloti++;
            return *this;
        }
        iterator operator++(int) // 后缀
        {
            iterator tmp(*this);
            operator++();
            return tmp;
        }
        bool operator==(const iterator &rhs) const
        {
            return blockid == rhs.blockid && sloti == rhs.sloti;
        }
        bool operator!=(const iterator &rhs) const
        {
            return blockid != rhs.blockid || sloti != rhs.sloti;
        }
        unsigned short slotid() { return sloti; }
        Record &operator*()
        {
            DataBlock block;
            block.attach(table.buffer_);
            unsigned short length = block.getFreeLength();
            unsigned short reoff = block.getSlot(sloti);
            record.attach(table.buffer_ + reoff, length);
            return record;
        }
    };

  public:
    using TableSpace = std::map<std::string, RelationInfo>;

  public:
    Table();
    ~Table();

  public:
    // 创建表
    int create(const char *name, RelationInfo &info);
    // 打开一张表
    int open(const char *name);
    //关闭一张表
    void close(const char *name);
    //摧毁一张表
    int destroy(const char *name);
    //初始化，读第1个block
    int initial();
    //读下一个block
    int openNextBlock();  //若不存在，则创建
    int openNextBlockE(); //若不存在，则返回错误
    //返回当前block的id
    int blockid();
    //写block
    int writeBlock();
    // 插入一条记录
    int insert(const unsigned char *header, struct iovec *record, int iovcnt);
    //删除一条记录
    int remove(struct iovec keyField);
    //更新一条记录
    int update(struct iovec keyField,const unsigned char *header, struct iovec *record, int iovcnt);

    // begin, end
    iterator begin()
    {
        DataBlock block;
        block.attach(buffer_);
        return iterator(block.blockid(), 0, *this);
    }
    iterator end()
    {
        DataBlock block;
        block.attach(buffer_);
        unsigned short slotsnum = block.getSlotsNum();
        return iterator(block.blockid(), slotsnum, *this);
    }

    // front,back
    Record &front() { return *begin(); }
    Record &back() { return *last(); }

  private:
    iterator last()
    {
        DataBlock block;
        block.attach(buffer_);
        unsigned short slotsnum = block.getSlotsNum();
        return iterator(block.blockid(), slotsnum - 1, *this);
    }
    Schema::TableSpace::iterator it; //表信息，map迭代器
    unsigned char *buffer_;          // block，TODO: 缓冲模块
};
struct Compare
{
  private:
    FieldInfo &fin;
    unsigned int key;
    Table &table;

  public:
    Compare(FieldInfo &f, unsigned int k, Table &itable)
        : fin(f)
        , key(k)
        , table(itable)
    {}
    bool operator()(const unsigned short &x, const unsigned short &y) const
    {
        //根据x, y偏移量，引用两条记录；
        Record rx, ry;
        rx.attach(table.buffer_ + x, Block::BLOCK_SIZE);
        ry.attach(table.buffer_ + y, Block::BLOCK_SIZE);
        iovec keyx, keyy;
        rx.specialRef(keyx, key);
        ry.specialRef(keyy, key);
        return fin.type->compare(
            keyx.iov_base, keyy.iov_base, keyx.iov_len, keyy.iov_len);
    }
};
} // namespace db

#endif // __DB_TABLE_H__
