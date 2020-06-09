////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/table.h>
namespace db {

Table::Table() { buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE); }
Table::~Table() { free(buffer_); }

int Table::create(const char *name, RelationInfo &info)
{
    return gschema.create(name, info);
}
int Table::open(const char *name)
{
    // 查找schema
    std::pair<Schema::TableSpace::iterator, bool> bret = gschema.lookup(name);
    if (!bret.second) return EINVAL;
    // 找到后，加载meta信息
    it = bret.first;
    gschema.load(it);
    return S_OK;
}
void Table::close(const char *name) { it->second.file.close(); }
int Table::destroy(const char *name) { return it->second.file.remove(name); }
int Table::initial()
{
    unsigned long long length;
    int ret = it->second.file.length(length);
    if (ret) return ret;
    // 加载
    if (length) {
        it->second.file.read(0, (char *) buffer_, Block::BLOCK_SIZE);
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        it->second.file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else {
        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_DATA);
        root.setHead(1);
        // 创建第1个block
        DataBlock block;
        block.attach(buffer_);
        block.clear(1);
        block.setNextid(-1); //?unsigned int?
        // 写root和block
        it->second.file.write(0, (const char *) rb, Root::ROOT_SIZE);
        it->second.file.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
}
int Table::openNextBlock()
{
    if (buffer_ == NULL) return S_FALSE;

    DataBlock block;
    block.attach(buffer_);
    int nextid = block.getNextid(); //下一个blockid
    int currid = block.blockid();   //目前的blockid
    if (nextid != -1) {
        size_t offset = (nextid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        it->second.file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else {
        block.clear(currid + 1);
        block.setNextid(-1);
        size_t offset = Root::ROOT_SIZE + currid * Block::BLOCK_SIZE;
        it->second.file.write(
            offset, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
}
int Table::openNextBlockE()
{
    if (buffer_ == NULL) return S_FALSE;

    DataBlock block;
    block.attach(buffer_);
    int nextid = block.getNextid(); //下一个blockid
    int currid = block.blockid();   //目前的blockid
    if (nextid != -1) {
        size_t offset = (nextid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        it->second.file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else
        return S_FALSE;
    return S_OK;
}
int Table::blockid()
{
    DataBlock block;
    block.attach(buffer_);
    return block.blockid();
}
int Table::writeBlock()
{
    DataBlock data;
    data.attach(buffer_);
    unsigned int blockid = data.blockid() - 1;
    size_t offset = blockid * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    it->second.file.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int Table::insert(const unsigned char *header, struct iovec *record, int iovcnt)
{
    //打开block
    bool ret = initial();
    if (ret) return ret;

    DataBlock data;
    data.attach(buffer_);
    while (1) {
        bool ret = data.allocate(header, record, iovcnt);
        if (!ret)
            //打开下一个block
            openNextBlock();
        else
            break;
    }

    // TODO:更新schema

    // 排序
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < data.getSlotsNum(); i++)
        slotsv.push_back(data.getSlot(i));
    unsigned int key = it->second.key;
    if (it->second.fields[key].type == NULL)
        it->second.fields[key].type =
            findDataType(it->second.fields[key].fieldType.c_str());
    Compare cmp(it->second.fields[key], key, *this);
    std::sort(slotsv.begin(), slotsv.end(), cmp);
    for (int i = 0; i < data.getSlotsNum(); i++)
        data.setSlot(i, slotsv[i]);

    // 处理checksum
    data.setChecksum();

    //写block
    ret = writeBlock();
    if (ret) return ret;
    return S_OK;
}

int Table::remove(struct iovec keyField)
{
    //打开block
    bool ret = initial();
    if (ret) return ret;

    //迭代所有block，判定主键的范围
    while (true) {
        DataBlock data;
        data.attach(buffer_);
        Record blockFront = front();
        Record blockBack = back();
        iovec keyFront, keyBack;
        unsigned int key = it->second.key;
        blockFront.specialRef(keyFront, key);
        blockBack.specialRef(keyBack, key);
        if (!it->second.fields[key].type->compare(
                keyField.iov_base,
                keyFront.iov_base,
                keyField.iov_len,
                keyFront.iov_len) &&
            !it->second.fields[key].type->compare(
                keyBack.iov_base,
                keyField.iov_base,
                keyBack.iov_len,
                keyField.iov_len))
            break;
        else {
            ret = openNextBlockE();
            if (ret) return ret;
        }
    }

    //读block.slots[]
    DataBlock data;
    data.attach(buffer_);
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < data.getSlotsNum(); i++)
        slotsv.push_back(data.getSlot(i));

    unsigned short slotid = -1;

    //如果record.key在正范围，则在block中查找
    for (auto iter = begin(); iter != end(); ++iter) {
        Record &record = *iter;
        iovec field;
        unsigned int key = it->second.key;
        record.specialRef(field, key);
        if (!(it->second.fields[key].type->compare(
                field.iov_base,
                keyField.iov_base,
                field.iov_len,
                keyField.iov_len)) &&
            !(it->second.fields[key].type->compare(
                keyField.iov_base,
                field.iov_base,
                keyField.iov_len,
                field.iov_len))) {
            slotid = iter.slotid();
        }
    }
    if (slotid == -1) return S_FALSE;
    // TODO:garbage pointer

    //删除slots
    auto eraseIt = slotsv.begin() + slotid;
    slotsv.erase(eraseIt);

    data.setSlotsNum(data.getSlotsNum() - 1);
    for (int i = 0; i < data.getSlotsNum(); i++)
        data.setSlot(i, slotsv[i]);

    // 处理checksum
    data.setChecksum();

    //写block
    ret = writeBlock();
    if (ret) return ret;
    return S_OK;
}
int Table::update(
    struct iovec keyField,
    const unsigned char *header,
    struct iovec *record,
    int iovcnt)
{
    bool cnt;
    cnt = remove(keyField);
    if (cnt) return cnt;
    cnt = insert(header, record, iovcnt);
    if (cnt) return cnt;
    return S_OK;
}
} // namespace db