////
// @file table.h
// @brief
// 存储管理
//
// @author junix
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

//比较
struct Compare;

//表
class Table
{
  public:
    //迭代器
    struct iterator;
    // 表的迭代器
    struct blockIter;

  public:
    //友元类声明
    friend struct Compare;
    friend struct iterator;
    friend struct blockIter;

  public:
    struct blockIter
    {
      private:
        unsigned int blockid; // block位置
        Table &table;
        DataBlock block;

      public:
        friend struct iterator;

      public:
        blockIter(unsigned int bid, Table &itable)
            : blockid(bid)
            , table(itable)
        {}
        blockIter(const blockIter &o)
            : blockid(o.blockid)
            , table(o.table)
        {}
        ~blockIter() {}
        unsigned int getBlockid() { return blockid; }
        blockIter &operator=(const blockIter &o)
        {
            blockid = o.blockid;
            table = o.table;
            return *this;
        }
        blockIter &operator++() // 前缀
        {
            size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            table.relationInfo->file.read(
                offset, (char *) table.buffer_, Block::BLOCK_SIZE);
            block.attach(table.buffer_);
            if (blockid != -1) blockid = block.getNextid();
            return *this;
        }
        blockIter operator++(int) // 后缀
        {
            blockIter tmp(*this);
            operator++();
            return tmp;
        }
        bool operator==(const blockIter &rhs) const
        {
            return blockid == rhs.blockid;
        }
        bool operator!=(const blockIter &rhs) const
        {
            return blockid != rhs.blockid;
        }
        DataBlock &operator*()
        {
            size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            table.relationInfo->file.read(
                offset, (char *) table.buffer_, Block::BLOCK_SIZE);
            block.attach(table.buffer_);
            return block;
        }
    };
    struct iterator
    {
      private:
        unsigned short sloti; // slots[]索引
        unsigned short slotmax;
        // Table &table;
        blockIter &blockit;
        Record record;

      public:
        iterator(unsigned short si, blockIter &iblockit)
            : sloti{si}
            , blockit(iblockit)
        {
            slotmax = (*blockit).getSlotsNum() - 1;
        }
        iterator(const iterator &o)
            : sloti(o.sloti)
            , blockit(o.blockit)
            , slotmax(o.slotmax)
        {}
        iterator &operator=(const iterator &o)
        {
            sloti = o.sloti;
            blockit = o.blockit;
            slotmax = o.slotmax;
            return *this;
        }
        iterator &operator++() // 前缀
        {
            if (sloti <= slotmax)
                sloti++;
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
            return sloti == rhs.sloti && blockit.blockid == rhs.blockit.blockid;
        }
        bool operator!=(const iterator &rhs) const
        {
            return sloti != rhs.sloti || blockit.blockid != rhs.blockit.blockid;
        }
        unsigned short getSlotid() { return sloti; }
        Record &operator*()
        {
            DataBlock block = *blockit;

            // while (block.getSlotsNum() == 0) {
            //     ++blockit;
            //     block = *blockit;
            // }
            unsigned short reoff = block.getSlot(sloti);
            record.attach(blockit.table.buffer_ + reoff, Block::BLOCK_SIZE);
            return record;
        }
    };

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
    //创建新datablock
    int creatDataBlock(int blockid, int &newid);
    //分裂datablock
    int splitDataBlock(int blockid);
    //!返回当前block的id,测试需要
    int blockid();
    //!返回当前block的空闲空间,测试需要
    unsigned short freelength();
    //!返回当前block的slotsNum,测试需要
    unsigned short slotsNum();
    //写block
    int writeBlock();
    //更新root
    int writeRoot();
    // 插入一条记录
    int insert(const unsigned char *header, struct iovec *record, int iovcnt);
    //删除一条记录
    int remove(struct iovec keyField);
    //更新一条记录
    int update(
        struct iovec keyField,
        const unsigned char *header,
        struct iovec *record,
        int iovcnt);
    // block begin、end
    blockIter blockBegin()
    {
        relationInfo->file.read(0, (char *) buffer_, Root::ROOT_SIZE);
        Root root;
        root.attach(buffer_);
        return blockIter(root.getHead(), *this);
    }
    blockIter blockEnd() { return blockIter(-1, *this); }
    // begin, end
    iterator begin(blockIter &blockIt) { return iterator(0, blockIt); }
    iterator end(blockIter &blockIt)
    {
        DataBlock block = *blockIt;
        unsigned short slotsnum = block.getSlotsNum();
        return iterator(slotsnum, blockIt);
    }

    // front,back
    Record &front(blockIter &blockIt) { return *begin(blockIt); }
    Record &back(blockIter &blockIt) { return *last(blockIt); }

  private:
    iterator last(blockIter &blockIt)
    {
        DataBlock block = *blockIt;
        unsigned short slotsnum = block.getSlotsNum();
        return iterator(slotsnum - 1, blockIt);
    }
    unsigned int DataBlockCnt;  // datablock数目
    RelationInfo *relationInfo; //表信息
    unsigned char *buffer_;     // block，TODO: 缓冲模块
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
