#include "ario/memory.h"

#include <stdexcept>
#include <utility>

namespace ario {

MemoryBlockAllocator::MemoryBlockAllocator() {}

MemoryBlockAllocator::MemoryBlockAllocator(size_t pool_bits, size_t block_bits)
    : pool_bits_(pool_bits),
      block_bits_(block_bits),
      pool_size_(1UL << pool_bits),
      block_size_(1UL << block_bits),
      blocks_(1UL << (pool_bits - block_bits)),
      rdma_lkey_(0),
      data_(new uint8_t[pool_size_]),
      availability_(blocks_, true) {
  available_blocks_.reserve(blocks_);
  for (size_t i = 0; i < blocks_; ++i) {
    available_blocks_.push_back(i);
  }
}

MemoryBlockAllocator::MemoryBlockAllocator(MemoryBlockAllocator &&other)
    : pool_bits_(std::exchange(other.pool_bits_, 0)),
      block_bits_(std::exchange(other.block_bits_, 0)),
      pool_size_(std::exchange(other.pool_size_, 0)),
      block_size_(std::exchange(other.block_size_, 0)),
      blocks_(std::exchange(other.blocks_, 0)),
      rdma_lkey_(std::exchange(other.rdma_lkey_, 0)),
      data_(std::move(other.data_)),
      available_blocks_(std::move(other.available_blocks_)),
      availability_(std::move(other.availability_)) {}

MemoryBlockAllocator &MemoryBlockAllocator::operator=(
    MemoryBlockAllocator &&other) {
  if (this == &other) return *this;
  pool_bits_ = std::exchange(other.pool_bits_, 0);
  block_bits_ = std::exchange(other.block_bits_, 0);
  pool_size_ = std::exchange(other.pool_size_, 0);
  block_size_ = std::exchange(other.block_size_, 0);
  blocks_ = std::exchange(other.blocks_, 0);
  rdma_lkey_ = std::exchange(other.rdma_lkey_, 0);
  data_ = std::move(other.data_);
  available_blocks_ = std::move(other.available_blocks_);
  availability_ = std::move(other.availability_);
  return *this;
}

OwnedMemoryBlock MemoryBlockAllocator::Allocate() {
  EnsureNonEmpty();
  std::lock_guard<std::mutex> lock(mutex_);
  if (available_blocks_.empty()) {
    throw std::bad_alloc();
  }
  auto index = available_blocks_.back();
  available_blocks_.pop_back();
  availability_[index] = false;
  auto *ptr = data_.get() + (index << block_bits_);
  return OwnedMemoryBlock(this, ptr, block_size_);
}

void MemoryBlockAllocator::Free(OwnedMemoryBlock &&block) {
  EnsureNonEmpty();
  if (block.allocator_ != this) {
    throw std::invalid_argument("block.allocator_ != this");
  }
  auto offset = block.data_ - data_.get();
  if (offset < 0) {
    throw std::out_of_range("offset < 0");
  }
  if (offset > pool_size_) {
    throw std::out_of_range("offset > pool_size_");
  }
  if (offset % (1ULL << block_bits_) != 0) {
    throw std::invalid_argument("offset not aligned with block size");
  }
  auto index = offset >> block_bits_;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (availability_[index]) {
      throw std::invalid_argument("double free");
    }
    availability_[index] = true;
    available_blocks_.push_back(index);
  }
  block.allocator_ = nullptr;
  block.data_ = nullptr;
  block.size_ = 0;
}

MessageView::MessageView(OwnedMemoryBlock &block) : block_(block) {}

uint8_t *MessageView::buf() { return block_.data_; }

MessageView::length_type MessageView::bytes_length() const {
  return *reinterpret_cast<length_type *>(block_.data_);
}

MessageView::length_type MessageView::max_bytes_length() const {
  return block_.size() - sizeof(length_type);
}

void MessageView::set_bytes_length(length_type bytes_length) {
  if (bytes_length + sizeof(length_type) > block_.size()) {
    throw std::out_of_range(
        "bytes_length + sizeof(length_type) > block_.size()");
  }
  *reinterpret_cast<length_type *>(block_.data_) = bytes_length;
}

uint8_t *MessageView::bytes() { return &block_.data_[sizeof(length_type)]; }

const uint8_t *MessageView::bytes() const {
  return &block_.data_[sizeof(length_type)];
}

MessageView::length_type MessageView::total_length() const {
  return sizeof(length_type) + bytes_length();
}

OwnedMemoryBlock::OwnedMemoryBlock()
    : allocator_(nullptr), data_(nullptr), size_(0) {}

OwnedMemoryBlock::OwnedMemoryBlock(MemoryBlockAllocator *allocator,
                                   uint8_t *data, size_t size)
    : allocator_(allocator), data_(data), size_(size) {}

OwnedMemoryBlock::OwnedMemoryBlock(OwnedMemoryBlock &&other)
    : allocator_(std::exchange(other.allocator_, nullptr)),
      data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)) {}

OwnedMemoryBlock &OwnedMemoryBlock::operator=(OwnedMemoryBlock &&other) {
  allocator_ = std::exchange(other.allocator_, nullptr);
  data_ = std::exchange(other.data_, nullptr);
  size_ = std::exchange(other.size_, 0);
  return *this;
}

OwnedMemoryBlock::~OwnedMemoryBlock() {
  if (allocator_) {
    allocator_->Free(std::move(*this));
  }
}

}  // namespace ario
