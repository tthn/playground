#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <atomic>
#include <thread>
#include <vector>
#include <cstdint>

template <class T, size_t SIZE>
class FifoQueue {
 public:
  struct Chunk {
    T data[SIZE];
    uint64_t chunk_version = 0;
    Chunk* next{nullptr};
  };

  FifoQueue() {
    begin_ = new Chunk();
    begin_->chunk_version = next_chunk_version_;
    next_chunk_version_ += 1;

    current_ = begin_;
  }

  ~FifoQueue() { }

  void Push(T&& value) {
    current_->data[in_chunk_index_] = std::move(value);
    in_chunk_index_ += 1;
    if (in_chunk_index_ >= SIZE) {
      auto* chunk = new Chunk;
      current_->next = chunk;
      current_ = chunk;
      current_->chunk_version = next_chunk_version_;
      next_chunk_version_ += 1;
      in_chunk_index_ = 0;

      uint64_t min_version = UINT64_MAX;
      for (auto** w : watermarks_) {
        auto v = (*w)->chunk_version;
        if (v < min_version) {
          min_version = v;
        }
      }

      chunk = begin_;
      while (chunk != nullptr) {
        auto* tmp = chunk->next;
        if (chunk->chunk_version < min_version) {
          delete chunk;
        } else {
          begin_ = chunk;
          break;
        }
        chunk = tmp;
      }
    }
  }

  struct Iterator {
    const T& data() {
      return chunk->data[index];
    }

    Chunk* chunk;
    int index;
  };

  void InitIterator(Iterator* iter) {
    iter->chunk = current_;
    iter->index = in_chunk_index_;
    watermarks_.push_back(&iter->chunk);
  }

  bool HasReady(Iterator* iter) {
    if (iter->index >= SIZE) {
      if (iter->chunk == current_) {
        return false;
      }

      iter->chunk = iter->chunk->next;
      iter->index = 0;
    }

    return iter->chunk != current_ || iter->index < in_chunk_index_;
  }

  void Next(Iterator* iter) {
    iter->index += 1;
  }

 private:
  std::vector<Chunk**> watermarks_;

  Chunk* begin_;
  int in_chunk_index_ = 0;
  Chunk* current_;

  uint64_t next_chunk_version_ = 1;
};

template <class Derived>
class Kicker {
 public:
  Kicker() {
    running_.store(0, std::memory_order_release);
    stopped_.store(0, std::memory_order_release);
  }

  void Notify() {
    int rc = running_.fetch_add(1, std::memory_order_acq_rel);
    if (rc == 0) {
      static_cast<Derived*>(this)->Kick();
    }
  }

  void Execute() {
    int count = 0;
    do {
      count = running_.load(std::memory_order_acquire);
      static_cast<Derived*>(this)->ExecuteOnce();

      if (stopped_.load(std::memory_order_acquire)) {
        return;
      }
    } while (MoreTask(count));
  }

 private:
  bool MoreTask(int count) {
    int rc = running_.fetch_sub(count, std::memory_order_release);
    if (rc - count == 0) {
      return false;
    }
    return true;
  }

  std::atomic<int> running_;
  std::atomic<int> stopped_;
};

using Queue = FifoQueue<int, 1024 * 1024>;

class Consumer : public Kicker<Consumer> {
 public:
  void Kick() {
    int c = 0;
    ::write(pipe_[1], &c, 1);
  }

  int Init(Queue* queue) {
    ::pipe2(pipe_, O_NONBLOCK);
    epfd_ = epoll_create(1024);
    if (epfd_ < 0) {
      return -1;
    }

    epoll_event event;
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = pipe_[0];
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, pipe_[0], &event) != 0) {
      return -1;
    }

    queue_ = queue;
    queue_->InitIterator(&iter_);
    return 0;
  }

  void ExecuteOnce() {
    while (queue_->HasReady(&iter_)) {
      round_ += 1;
      if (round_ % (128 * 1024) == 0) {
        int64_t delta = MonoTimeNs() - start_;
        printf("round:%lu tm:%ld result:%lu\n", round_, delta, result_);
      }
      result_ += iter_.chunk->data[iter_.index];
      queue_->Next(&iter_);
    }
  }

  void Run() {
    start_ = MonoTimeNs();

    char c[4096];
    while (true) {
      epoll_event event;
      int rc = epoll_wait(epfd_, &event, 1, -1);
      if (rc < 0) {
        printf("fail to wait: %m\n");
      }

      if (::read(event.data.fd, c, 4096) > 0) {
        Execute();
      }
    }
  }

 private:
  uint64_t MonoTimeNs() {
    timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return tp.tv_sec * 1000'000'000 + tp.tv_nsec;
  }

  Queue::Iterator iter_;
  Queue* queue_;

  int pipe_[2];
  int epfd_ = -1;

  uint64_t round_ = 0;
  uint64_t result_ = 0;

  uint64_t start_;
};

void Produce(Queue* queue, Consumer* consumer) {
  uint64_t i = 0;
  while (true) {
    queue->Push(i);
    consumer->Notify();
    i += 1;
  }
}

int main() {
  Queue* q = new Queue;
  Consumer c;
  c.Init(q);
  std::thread pth([q, &c] {
    Produce(q, &c);
  });

  std::thread cth([&c] {
    c.Run();
  });

  pth.join();
  cth.join();
}

