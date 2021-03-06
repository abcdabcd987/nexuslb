#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace ario {

template <typename T>
class Span {
 public:
  Span(T *data, size_t size) : data_(data), size_(size) {}
  T *data() const { return data_; }
  size_t size() const { return size_; }

 private:
  T *data_;
  size_t size_;
};

using MutableBuffer = Span<void>;
using ConstBuffer = Span<const void>;

class OwnedMemoryBlock;

class MemoryBlockAllocator {
 public:
  MemoryBlockAllocator();
  MemoryBlockAllocator(size_t pool_bits, size_t block_bits);
  MemoryBlockAllocator(const MemoryBlockAllocator &other) = delete;
  MemoryBlockAllocator &operator=(const MemoryBlockAllocator &other) = delete;
  MemoryBlockAllocator(MemoryBlockAllocator &&other);
  MemoryBlockAllocator &operator=(MemoryBlockAllocator &&other);

  bool empty() const { return data_.get() == nullptr; }
  size_t pool_bits() const { return pool_bits_; }
  size_t block_bits() const { return block_bits_; }
  size_t pool_size() const { return pool_size_; }
  size_t block_size() const { return block_size_; }
  size_t blocks() const { return blocks_; }
  uint8_t *data() const { return data_.get(); }
  uint32_t rdma_lkey() const { return rdma_lkey_; }
  void set_rdma_lkey(uint32_t rdma_lkey) { rdma_lkey_ = rdma_lkey; }

  OwnedMemoryBlock Allocate() /* REQUIRES(mutex_) */;
  void Free(OwnedMemoryBlock &&block) /* REQUIRES(mutex_) */;

 private:
  void EnsureNonEmpty() const {
    if (empty()) {
      throw std::out_of_range("MemoryBlockAllocator is empty");
    }
  }

  size_t pool_bits_ = 0;
  size_t block_bits_ = 0;
  size_t pool_size_ = 0;
  size_t block_size_ = 0;
  size_t blocks_ = 0;
  uint32_t rdma_lkey_ = 0;
  std::unique_ptr<uint8_t[]> data_;

  std::mutex mutex_;
  std::vector<size_t> available_blocks_ /* GUARDED_BY(mutex_) */;
  std::vector<bool> availability_ /* GUARDED_BY(mutex_) */;
};

class MessageView {
 public:
  explicit MessageView(OwnedMemoryBlock &block);
  MessageView(const MessageView &other) = delete;
  MessageView &operator=(const MessageView &other) = delete;
  MessageView(MessageView &&other) = delete;
  MessageView &operator=(MessageView &&other) = delete;

  using length_type = uint32_t;
  uint8_t *buf();
  length_type bytes_length() const;
  length_type max_bytes_length() const;
  void set_bytes_length(length_type bytes_length);
  uint8_t *bytes();
  const uint8_t *bytes() const;
  length_type total_length() const;

 private:
  OwnedMemoryBlock &block_;
};

class OwnedMemoryBlock {
 public:
  OwnedMemoryBlock();
  ~OwnedMemoryBlock();
  OwnedMemoryBlock(const OwnedMemoryBlock &other) = delete;
  OwnedMemoryBlock &operator=(const OwnedMemoryBlock &other) = delete;
  OwnedMemoryBlock(OwnedMemoryBlock &&other);
  OwnedMemoryBlock &operator=(OwnedMemoryBlock &&other);

  bool empty() const { return allocator_ == nullptr; }

  MemoryBlockAllocator *allocator() const {
    EnsureNonEmpty();
    return allocator_;
  }

  uint8_t *data() const {
    EnsureNonEmpty();
    return data_;
  }

  size_t size() const {
    EnsureNonEmpty();
    return size_;
  }

  uint32_t rdma_lkey() const {
    EnsureNonEmpty();
    return allocator_->rdma_lkey();
  }

  MessageView AsMessageView() {
    EnsureNonEmpty();
    return MessageView(*this);
  }

 private:
  friend class MemoryBlockAllocator;
  friend class MessageView;
  OwnedMemoryBlock(MemoryBlockAllocator *allocator, uint8_t *data, size_t size);

  void EnsureNonEmpty() const {
    if (empty()) {
      throw std::out_of_range("OwnedMemoryBlock is empty");
    }
  }

  MemoryBlockAllocator *allocator_;
  uint8_t *data_;
  size_t size_;
};

}  // namespace ario
