#include "nexus/common/connection.h"

#include <glog/logging.h>

namespace nexus {

Connection::Connection(boost::asio::ip::tcp::socket socket,
                       MessageHandler* handler)
    : socket_(std::move(socket)), handler_(handler), wrong_header_(false) {
  boost::asio::ip::tcp::no_delay option(true);
  socket_.set_option(option);
}

Connection::Connection(boost::asio::io_context& io_context,
                       MessageHandler* handler)
    : socket_(io_context), handler_(handler), wrong_header_(false) {}

void Connection::Start() { DoReadHeader(); }

void Connection::Stop() {
  LOG(INFO) << "Connection Stop";
  std::lock_guard<std::mutex> socket_guard(socket_mutex_);
  socket_.close();
}

Connection::~Connection() {
  if (socket_.is_open()) {
    Stop();
  }
}

void Connection::Write(std::shared_ptr<Message> msg) {
  std::lock_guard<std::mutex> lock(write_queue_mutex_);
  bool write_in_progress = !write_queue_.empty();
  write_queue_.push_back(std::move(msg));
  if (!write_in_progress) {
    DoWrite();
  }
}

void Connection::DoReadHeader() {
  auto self(shared_from_this());
  std::lock_guard<std::mutex> socket_guard(socket_mutex_);
  boost::asio::async_read(
      socket_, boost::asio::buffer(msg_header_buffer_, MESSAGE_HEADER_SIZE),
      [this, self](boost::system::error_code ec, size_t nbytes) {
        if (ec) {
          if (ec != boost::asio::error::operation_aborted) {
            handler_->HandleError(self, ec);
          }
          return;
        }
        MessageHeader msg_header;
        if (!DecodeHeader(msg_header_buffer_, &msg_header)) {
          if (!wrong_header_) {
            LOG(ERROR) << "Wrong header detected";
            wrong_header_ = true;
          }
          DoReadHeader();
        } else {
          wrong_header_ = false;
          auto msg = std::make_shared<Message>(msg_header);
          // LOG(INFO) << "msg type: " << msg->type() << ", body length: " <<
          //     msg->body_length();
          DoReadBody(std::move(msg));
        }
      });
}

void Connection::DoReadBody(std::shared_ptr<Message> msg) {
  auto self(shared_from_this());
  std::lock_guard<std::mutex> socket_guard(socket_mutex_);
  boost::asio::async_read(socket_,
                          boost::asio::buffer(msg->body(), msg->body_length()),
                          [this, self, msg](boost::system::error_code ec,
                                            size_t /* bytes_transferred */) {
                            if (ec) {
                              if (ec != boost::asio::error::operation_aborted) {
                                handler_->HandleError(self, ec);
                              }
                            } else {
                              handler_->HandleMessage(self, std::move(msg));
                              DoReadHeader();
                            }
                          });
}

void Connection::DoWrite() {
  auto self(shared_from_this());
  std::lock_guard<std::mutex> socket_guard(socket_mutex_);
  boost::asio::async_write(
      socket_,
      boost::asio::buffer(write_queue_.front()->data(),
                          write_queue_.front()->length()),
      [this, self](boost::system::error_code ec, size_t) {
        std::lock_guard<std::mutex> lock(write_queue_mutex_);
        if (ec) {
          if (ec != boost::asio::error::operation_aborted) {
            handler_->HandleError(self, ec);
          }
        } else {
          write_queue_.pop_front();
          if (!write_queue_.empty()) {
            DoWrite();
          }
        }
      });
}

}  // namespace nexus
