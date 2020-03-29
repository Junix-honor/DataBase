////
// @file record.cc
// @brief
// 实现物理记录
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <vector>
#include <db/record.h>


namespace db {

// TODO: 加上log
std::pair<size_t, size_t> Record::size(const iovec *iov, int iovcnt)
{
    size_t s1 = 0, s2 = 0;

    Integer it;
    for (int i = 0; i < iovcnt; ++i) {
        it.set(iov[i].iov_len);
        s1 += it.size() + iov[i].iov_len;
        s2 += it.size();
    }
    s1 += 1; // header
    it.set(s1);
    s1 += it.size(); // 整个记录长度
    s2 += it.size();

    return std::pair<size_t, size_t>(s1, s2);
}

bool Record::set(const iovec *iov, int iovcnt, const unsigned char *header)
{
    // 先计算所需空间大小
    std::pair<size_t, size_t> s1 = size(iov, iovcnt);
    if (this->left() < s1.first) return false;

    // 输出记录长度
    Integer it;
    it.set(s1.first);
    it.encode(this->buffer_ + this->offset_, this->left());
    this->offset_ += it.size();

    // 输出字段偏移量数组
    size_t len = HEADER_SIZE;
    for (int i = 0; i < iovcnt; ++i)
        len += iov[i].iov_len; // 计算总长
    // 逆序输出
    for (int i = iovcnt; i > 0; --i) {
        len -= iov[i - 1].iov_len;
        it.set(len);
        it.encode(this->buffer_ + this->offset_, this->left());
        this->offset_ += it.size();
    }

    // 输出头部
    memcpy(this->buffer_ + this->offset_, header, HEADER_SIZE);
    this->offset_ += HEADER_SIZE;

    // 顺序输出各字段
    for (int i = 0; i < iovcnt; ++i) {
        memcpy(this->buffer_ + this->offset_, iov[i].iov_base, iov[i].iov_len);
        this->offset_ += iov[i].iov_len;
    }

    // 输出padding
    size_t s2 = this->offset_ % ALIGN_SIZE;
    if (s2)
        for (size_t i = 0; i < ALIGN_SIZE - s2; ++i) {
            this->buffer_[this->offset_] = 0;
            ++this->offset_;
        }

    return true;
}

size_t Record::length(size_t offset)
{
    if (offset > this->length_) return 0;
    Integer it;
    bool ret = it.decode(this->buffer_ + offset, this->length_ - offset);
    if (!ret) return 0;
    return it.value_;
}

size_t Record::fields(size_t offset)
{
    // bypass总长
    if (offset > this->length_) return 0;
    Integer it;
    bool ret = it.decode(this->buffer_ + offset, this->length_ - offset);
    if (!ret) return 0;
    offset += it.size();

    // 枚举所有字段长度
    size_t total = 0;
    while (true) {
        if (offset >= this->length_) return 0;
        ret = it.decode(this->buffer_ + offset, this->length_ - offset);
        if (!ret)
            return 0;
        else
            offset += it.size();

        ++total;
        // 找到尾部
        if (it.value_ == (unsigned long) HEADER_SIZE) break;
    }

    return total;
}

bool Record::get(iovec *iov, int iovcnt, unsigned char *header)
{
    if (header == NULL) return false;
    size_t offset = this->offset_;

    // 总长
    size_t length = 0;
    Integer it;
    bool ret =
        it.decode(this->buffer_ + this->offset_, this->length_ - this->offset_);
    if (!ret) return false;
    length = it.get();

    // 枚举所有字段长度
    size_t total = 0;
    this->offset_ += it.size();
    std::vector<size_t> vec(iovcnt);
    while (true) {
        if (this->offset_ >= this->length_) return false;
        ret = it.decode(
            this->buffer_ + this->offset_, this->length_ - this->offset_);
        if (!ret)
            return false;
        else
            this->offset_ += it.size();

        vec[total] = it.get(); // 临时存放偏移量，注意是逆序
        if (++total > (size_t) iovcnt) return false;

        // 找到尾部
        if (it.value_ == (unsigned long) HEADER_SIZE) break;
    }
    if (total != (size_t) iovcnt) return false; // 字段数目不对
    // 逆序，先交换
    for (int i = 0; i < iovcnt / 2; ++i) {
        size_t tmp = vec[i];
        vec[i] = vec[iovcnt - i - 1];
        vec[iovcnt - i - 1] = tmp;
    }
    // check长度
    for (int i = 0; i < iovcnt - 1; ++i) {
        vec[i] = vec[i + 1] - vec[i];
        if (vec[i] != iov[i].iov_len) return false;
    }
    vec[iovcnt - 1] = length - vec[iovcnt - 1] - (this->offset_ - offset);
    if (vec[iovcnt - 1] != iov[iovcnt - 1].iov_len) return false;

    // 拷贝header
    memcpy(header, this->buffer_ + this->offset_, HEADER_SIZE);
    this->offset_ += HEADER_SIZE;

    // 拷贝字段
    for (int i = 0; i < iovcnt; ++i) {
        memcpy(iov[i].iov_base, this->buffer_ + this->offset_, iov[i].iov_len);
        this->offset_ += iov[i].iov_len;
    }

    // padding
    size_t s2 = this->offset_ % ALIGN_SIZE;
    if (s2) this->offset_ += ALIGN_SIZE - s2;

    return true;
}

} // namespace db
