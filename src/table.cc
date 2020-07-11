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

Table::Table()
    : relationInfo(NULL)
{
    buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
}
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
    gschema.load(bret.first);
    relationInfo = &bret.first->second;
    return S_OK;
}
void Table::close(const char *name) { relationInfo->file.close(); }
int Table::destroy(const char *name) { return relationInfo->file.remove(name); }
int Table::initial()
{
    unsigned long long length;
    int ret = relationInfo->file.length(length);
    if (ret) return ret;
    // 加载
    if (length) {
        relationInfo->file.read(0, (char *) buffer_, Root::ROOT_SIZE);
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        DataBlockCnt = root.getCnt();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        relationInfo->file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
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
        block.setNextid(-1);
        DataBlockCnt = 1;
        root.setCnt(DataBlockCnt);
        // 写root和block
        relationInfo->file.write(0, (const char *) rb, Root::ROOT_SIZE);
        relationInfo->file.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
}
// int Table::creatDataBlock(int blockid, int &newid)
// {
//     int nextid;
//     if (blockid == 0) {
//         relationInfo->file.read(0, (char *) buffer_, Root::ROOT_SIZE);
//         Root root;
//         root.attach(buffer_);
//         nextid = root.getHead();
//         root.setHead(++DataBlockCnt);
//         root.setCnt(DataBlockCnt);
//     } else {
//         DataBlock block;
//         size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
//         relationInfo->file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
//         block.attach(buffer_);
//         nextid = block.getNextid();
//         block.setNextid(++DataBlockCnt);
//     }
//     DataBlock block;
//     block.clear(DataBlockCnt);
//     block.setNextid(nextid);
//     newid = DataBlockCnt;
//     int ret = writeBlock();
//     if (ret) return ret;
//     ret = writeRoot();
//     if (ret) return ret;
//     return S_OK;
// }
int Table::splitDataBlock(int blockid)
{
    //原block
    int nextid;
    DataBlock block;
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->file.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    block.attach(buffer_);
    nextid = block.getNextid();

    //分裂的新block
    DataBlock newBlock1, newBlock2;
    unsigned char db1[Block::BLOCK_SIZE];
    unsigned char db2[Block::BLOCK_SIZE];
    newBlock1.attach(db1);
    newBlock1.clear(block.blockid());
    newBlock1.setNextid(++DataBlockCnt);
    newBlock2.attach(db2);
    newBlock2.clear(DataBlockCnt);
    newBlock2.setNextid(nextid);

    unsigned short slotsNum = block.getSlotsNum();
    for (unsigned short index = 0; index < slotsNum / 2; index++) {
        unsigned short recOffset = block.getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);
        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);

        newBlock1.allocate(&header, iov, (int) fields);
        free(iov);
    }

    for (unsigned short index = slotsNum / 2; index < slotsNum; index++) {
        unsigned short recOffset = block.getSlot(index);
        Record record;
        record.attach(buffer_ + recOffset, Block::BLOCK_SIZE);

        // 先分配iovec
        size_t fields = record.fields();
        struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
        unsigned char header;
        // 从记录得到iovec
        record.ref(iov, (int) fields, &header);

        newBlock2.allocate(&header, iov, (int) fields);
        free(iov);
    }

    //写block
    offset = (newBlock1.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->file.write(offset, (const char *) db1, Block::BLOCK_SIZE);

    offset = (newBlock2.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->file.write(offset, (const char *) db2, Block::BLOCK_SIZE);

    //更新root
    int ret = writeRoot();
    if (ret) return ret;
    return S_OK;
}
int Table::blockid()
{
    DataBlock block;
    block.attach(buffer_);
    return block.blockid();
}
unsigned short Table::freelength()
{
    DataBlock block;
    block.attach(buffer_);
    return block.getFreeLength();
}
unsigned short Table::slotsNum()
{
    DataBlock block;
    block.attach(buffer_);
    return block.getSlotsNum();
}
int Table::writeBlock()
{
    DataBlock data;
    data.attach(buffer_);
    unsigned int blockid = data.blockid();
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    relationInfo->file.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int Table::writeRoot()
{
    relationInfo->file.read(0, (char *) buffer_, Root::ROOT_SIZE);
    Root root;
    root.attach(buffer_);
    root.setCnt(DataBlockCnt);
    relationInfo->file.write(0, (const char *) buffer_, Root::ROOT_SIZE);
    return S_OK;
}
int Table::insert(const unsigned char *header, struct iovec *record, int iovcnt)
{
    //打开block
    bool ret = initial();
    if (ret) return ret;
    unsigned int key = relationInfo->key;
    iovec &keyField = record[key];
    DataBlock data;

    for (auto bit1 = blockBegin(), bit2 = ++blockBegin(); bit1 != blockEnd();
         ++bit1, ++bit2) {
        if (bit2 == blockEnd()) {
            data = *bit1;
            bool ret = data.allocate(header, record, iovcnt);
            if (!ret) {
                splitDataBlock(data.blockid());
                insert(header, record, iovcnt);
                data = *bit1;
            }
            break;
        }
        data = *bit1;
        if (data.getSlotsNum() == 0) continue;

        iovec key1, key2;
        //获取key2字段
        Record rec = *iterator(0, bit2);
        rec.specialRef(key2, key);

        //获取key1字段
        data = *bit1;
        rec = *iterator(0, bit1);
        rec.specialRef(key1, key);

        if (relationInfo->fields[key].type->compare(
                keyField.iov_base,
                key2.iov_base,
                keyField.iov_len,
                key2.iov_len) &&
            (relationInfo->fields[key].type->compare(
                key1.iov_base,
                keyField.iov_base,
                key1.iov_len,
                keyField.iov_len))) {
            data = *bit1;
            bool ret = data.allocate(header, record, iovcnt);
            if (!ret) {
                splitDataBlock(data.blockid());
                insert(header, record, iovcnt);
                data = *bit1;
            }
            break;
        } else if (
            relationInfo->fields[key].type->compare(
                keyField.iov_base,
                key1.iov_base,
                keyField.iov_len,
                key1.iov_len) &&
            bit1 == blockBegin()) {
            data = *bit1;
            bool ret = data.allocate(header, record, iovcnt);
            if (!ret) {
                splitDataBlock(data.blockid());
                insert(header, record, iovcnt);
                data = *bit1;
            }
            break;
        }
    }

    // TODO:更新schema

    // 排序
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < data.getSlotsNum(); i++)
        slotsv.push_back(data.getSlot(i));
    if (relationInfo->fields[key].type == NULL)
        relationInfo->fields[key].type =
            findDataType(relationInfo->fields[key].fieldType.c_str());
    Compare cmp(relationInfo->fields[key], key, *this);
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
    unsigned int key = relationInfo->key;
    DataBlock data;
    int blockid;
    auto bit = blockBegin();
    for (; bit != blockEnd(); ++bit) {
        data = *bit;
        if (data.getSlotsNum() == 0) continue;

        iovec keyFront, keyBack;
        Record recFront = front(bit);
        recFront.specialRef(keyFront, key);
        Record reckBack = back(bit);
        reckBack.specialRef(keyBack, key);
        if (!relationInfo->fields[key].type->compare(
                keyField.iov_base,
                keyFront.iov_base,
                keyField.iov_len,
                keyFront.iov_len) &&
            !relationInfo->fields[key].type->compare(
                keyBack.iov_base,
                keyField.iov_base,
                keyBack.iov_len,
                keyField.iov_len)) {
            blockid = bit.getBlockid();
            break;
        }
    }
    if (bit == blockEnd()) return S_FALSE;
    //读block.slots[]
    data.attach(buffer_);
    std::vector<unsigned short> slotsv;
    for (int i = 0; i < data.getSlotsNum(); i++)
        slotsv.push_back(data.getSlot(i));

    unsigned short slotid = -1;

    //如果record.key在正范围，则在block中查找
    for (auto iter = begin(bit); iter != end(bit); ++iter) {
        Record &record = *iter;
        iovec field;
        unsigned int key = relationInfo->key;
        record.specialRef(field, key);
        if (!(relationInfo->fields[key].type->compare(
                field.iov_base,
                keyField.iov_base,
                field.iov_len,
                keyField.iov_len)) &&
            !(relationInfo->fields[key].type->compare(
                keyField.iov_base,
                field.iov_base,
                keyField.iov_len,
                field.iov_len))) {
            slotid = iter.getSlotid();
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
    int ret;
    ret = remove(keyField);
    if (ret) return ret;
    ret = insert(header, record, iovcnt);
    if (ret) return ret;
    return S_OK;
}
} // namespace db