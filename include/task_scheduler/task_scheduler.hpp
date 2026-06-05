#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ts {

// ── forward ──────────────────────────────────────────────────────────────────

class TaskScheduler;

// ── config & enums ───────────────────────────────────────────────────────────

enum class SubmitPolicy { BLOCK, TIMEOUT, REJECT };

struct SchedulerConfig {
    std::size_t thread_num{4};
    std::size_t queue_capacity{1024};
    SubmitPolicy submit_policy{SubmitPolicy::BLOCK};
    std::chrono::milliseconds submit_timeout{100};
};

enum TaskPriority : int { LOW = 0, NORMAL = 1, HIGH = 2 };

// ── stats ─────────────────────────────────────────────────────────────────────

class SchedulerStats {
public:
    auto submitted_count() const noexcept { return submitted_.load(); }
    auto completed_count() const noexcept { return completed_.load(); }
    auto rejected_count()  const noexcept { return rejected_.load(); }
    auto queued_count()   const noexcept { return queued_.load(); }

    void add_submitted(std::uint64_t n = 1) noexcept { submitted_.fetch_add(n); }
    void add_completed (std::uint64_t n = 1) noexcept { completed_.fetch_add(n); }
    void add_rejected  (std::uint64_t n = 1) noexcept { rejected_.fetch_add(n); }
    void add_queued    (std::uint64_t n = 1) noexcept { queued_.fetch_add(n); }
    void sub_queued    (std::uint64_t n = 1) noexcept { queued_.fetch_sub(n); }
    void add_serial_submitted() noexcept { serial_submitted_.fetch_add(1); }
    void add_serial_completed() noexcept { serial_completed_.fetch_add(1); }
    bool serial_all_done() const noexcept {
        return serial_submitted_.load() == serial_completed_.load();
    }
    void sub_queued_and_serial_completed() noexcept {
        queued_.fetch_sub(1);
        completed_.fetch_add(1);
    }

private:
    std::atomic<std::uint64_t> submitted_{0};
    std::atomic<std::uint64_t> completed_{0};
    std::atomic<std::uint64_t> rejected_{0};
    std::atomic<std::uint64_t> queued_{0};
    std::atomic<std::uint64_t> serial_submitted_{0};
    std::atomic<std::uint64_t> serial_completed_{0};
};

// ── task node ────────────────────────────────────────────────────────────────

struct TaskNode {
    int priority{static_cast<int>(TaskPriority::NORMAL)};
    std::uint64_t sequence{0};
    std::function<void()> func;

    bool operator<(const TaskNode& other) const noexcept {
        if (priority != other.priority) return priority < other.priority;
        return sequence < other.sequence;   // smaller seq = submitted earlier
    }
};

// ── blocking priority queue (lock-guarded, supports cancel) ──────────────────

class PriorityQueue {
public:
    explicit PriorityQueue(std::size_t capacity, SchedulerStats* stats = nullptr,
                           std::chrono::milliseconds block_timeout = std::chrono::milliseconds(0))
        : capacity_(capacity), stats_(stats), block_timeout_(block_timeout) {}

    // Returns false only when rejected by policy
    bool push(TaskNode node) {
        std::unique_lock<std::mutex> lock(mtx_);
        // 0 == infinite block (default for BLOCK policy)
        auto deadline = (block_timeout_.count() > 0)
                            ? std::chrono::steady_clock::now() + block_timeout_
                            : std::chrono::steady_clock::time_point::max();
        while (queue_.size() >= capacity_ && !stopped_) {
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                if (stats_) stats_->add_rejected();
                return false;
            }
        }
        if (stopped_) { if (stats_) stats_->add_rejected(); return false; }
        queue_.push(std::move(node));
        cv_.notify_all();
        return true;
    }

    // Non-blocking — caller counts rejected on false
    bool try_push(TaskNode node) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (queue_.size() >= capacity_ || stopped_) return false;
        queue_.push(std::move(node));
        cv_.notify_all();
        return true;
    }

    // Blocking wait-pop; returns nullopt when stopped
    std::optional<TaskNode> wait_pop() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return !queue_.empty() || stopped_; });
        if (queue_.empty()) return std::nullopt;
        auto node = std::move(const_cast<TaskNode&>(queue_.top()));
        queue_.pop();
        cv_.notify_all();   // wake ALL — workers and blocked producers
        return node;
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mtx_);
        stopped_ = true;
        cv_.notify_all();
    }

    [[nodiscard]] bool empty() const noexcept {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.empty();
    }

private:
    const std::size_t capacity_;
    SchedulerStats* const stats_{nullptr};
    const std::chrono::milliseconds block_timeout_{0};
    std::priority_queue<TaskNode> queue_;
    bool stopped_{false};
    mutable std::mutex mtx_;
    std::condition_variable cv_;
};

// ── delayed task ─────────────────────────────────────────────────────────────

struct DelayedTask {
    std::chrono::steady_clock::time_point run_time;
    std::function<void()> func;
    std::uint64_t sequence{0};

    bool operator<(const DelayedTask& other) const noexcept {
        // std::priority_queue is a max-heap by default, so we reverse for min-heap
        return run_time > other.run_time;
    }
};

// ── per-key shard (serial execution) ─────────────────────────────────────────

class KeyShard {
public:
    explicit KeyShard(std::size_t capacity) : queue_(capacity) {}

    bool enqueue(std::function<void()> task) {
        if (!queue_.push(std::move(task))) return false;
        pending_flag_.store(true, std::memory_order_release);
        return true;
    }

    bool dequeue(std::function<void()>& task) {
        bool ok = queue_.pop(task);
        if (ok && queue_.empty()) {
            pending_flag_.store(false, std::memory_order_release);
        }
        return ok;
    }

    // Non-mutex check for wait — safe with atomic flag + circular buffer
    [[nodiscard]] bool has_pending() const noexcept {
        return pending_flag_.load(std::memory_order_acquire);
    }

    void process_all(std::function<void()> on_complete);

private:
    // Simple bounded queue for shard (no priority needed, serial by nature)
    class BQueue {
    public:
        explicit BQueue(std::size_t cap) : cap_(cap), data_(cap) {}
        bool push(std::function<void()> item) {
            std::lock_guard<std::mutex> l(mtx_);
            if (count_ >= cap_) return false;
            data_[tail_] = std::move(item);
            tail_ = (tail_ + 1) % cap_;
            ++count_;
            return true;
        }
        bool pop(std::function<void()>& item) {
            std::lock_guard<std::mutex> l(mtx_);
            if (count_ == 0) return false;
            item = std::move(data_[head_]);
            head_ = (head_ + 1) % cap_;
            --count_;
            return true;
        }
        [[nodiscard]] bool empty() const { std::lock_guard<std::mutex> l(mtx_); return count_ == 0; }

    private:
        std::size_t cap_;
        std::vector<std::function<void()>> data_;
        std::size_t head_{0}, tail_{0}, count_{0};
        mutable std::mutex mtx_;
    };

    BQueue queue_;
    std::atomic<bool> pending_flag_{false};
};

// ── main scheduler ────────────────────────────────────────────────────────────

class TaskScheduler {
public:
    explicit TaskScheduler(const SchedulerConfig& cfg = {});
    ~TaskScheduler();

    TaskScheduler(const TaskScheduler&) = delete;
    TaskScheduler& operator=(const TaskScheduler&) = delete;
    TaskScheduler(TaskScheduler&&) = delete;
    TaskScheduler& operator=(TaskScheduler&&) = delete;

    // ── submit ─────────────────────────────────────────────────────────────

    template <typename F>
    bool submit(TaskPriority p, F&& f);

    template <typename F, typename... Args>
    auto submit_with_result(TaskPriority p, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>;

    template <typename F>
    bool submit(int priority, F&& f) {
        return submit(static_cast<TaskPriority>(priority), std::forward<F>(f));
    }

    // ── delay ──────────────────────────────────────────────────────────────

    template <typename F>
    bool schedule_after(TaskPriority p, std::chrono::milliseconds delay, F&& f);

    // ── serial ─────────────────────────────────────────────────────────────

    template <typename F>
    bool submit_serial(const std::string& key, F&& f);

    // ── control ───────────────────────────────────────────────────────────

    void shutdown() noexcept;
    void await_until_empty() const;

    // ── stats ─────────────────────────────────────────────────────────────

    SchedulerStats& stats() noexcept { return stats_; }
    const SchedulerStats& stats() const noexcept { return stats_; }
    [[nodiscard]] bool is_running() const noexcept { return running_.load(); }

private:
    void worker_loop();
    void timer_loop();
    void key_serial_loop();

    SchedulerConfig config_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};

    PriorityQueue task_queue_{config_.queue_capacity, &stats_, config_.submit_timeout};
    std::vector<std::thread> workers_;
    std::thread timer_thread_;
    std::thread key_thread_;

    // delay heap
    std::priority_queue<DelayedTask> delay_heap_;
    std::mutex delay_mtx_;
    std::condition_variable delay_cv_;

    // key shards — permanent once created
    std::unordered_map<std::string, std::unique_ptr<KeyShard>> key_shards_;
    std::unordered_set<std::string> active_keys_;   // keys that have pending work
    std::mutex key_mtx_;
    std::condition_variable key_cv_;

    std::atomic<std::uint64_t> sequence_{0};
    SchedulerStats stats_{};

    static constexpr std::size_t MAX_SHARD_CAPACITY = 4096;
};

// ── template implementations ──────────────────────────────────────────────────

template <typename F>
bool TaskScheduler::submit(TaskPriority p, F&& f) {
    if (stopping_.load(std::memory_order_acquire)) return false;

    TaskNode node;
    node.priority = static_cast<int>(p);
    node.sequence = sequence_.fetch_add(1, std::memory_order_relaxed);
    node.func = std::forward<F>(f);

    bool ok = (config_.submit_policy == SubmitPolicy::REJECT)
                ? task_queue_.try_push(std::move(node))
                : task_queue_.push(std::move(node));
    if (ok) {
        stats_.add_submitted();
        stats_.add_queued();
    } else {
        stats_.add_rejected();
    }
    return ok;
}

template <typename F, typename... Args>
auto TaskScheduler::submit_with_result(TaskPriority p, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>> {
    using R = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;
    auto task = std::make_shared<std::packaged_task<R()>>(
        [f = std::forward<F>(f), tup = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            return std::apply(f, tup);
        });
    auto fut = task->get_future();
    submit(p, [t = std::move(task)]() { (*t)(); });
    return fut;
}

template <typename F>
bool TaskScheduler::schedule_after(TaskPriority p, std::chrono::milliseconds delay, F&& f) {
    if (stopping_.load(std::memory_order_acquire)) return false;
    {
        std::lock_guard<std::mutex> lock(delay_mtx_);
        DelayedTask dt;
        dt.run_time = std::chrono::steady_clock::now() + delay;
        dt.sequence = sequence_.fetch_add(1, std::memory_order_relaxed);
        dt.func = [this, f = std::forward<F>(f)]() mutable {
            f();
            stats_.add_completed();
        };
        delay_heap_.push(std::move(dt));
        stats_.add_submitted();
    }
    delay_cv_.notify_one();
    return true;
}

template <typename F>
bool TaskScheduler::submit_serial(const std::string& key, F&& f) {
    if (stopping_.load(std::memory_order_acquire)) return false;

    std::unique_ptr<KeyShard>& shard_ptr = [this, &key]() -> std::unique_ptr<KeyShard>& {
        std::lock_guard<std::mutex> lock(key_mtx_);
        auto it = key_shards_.find(key);
        if (it != key_shards_.end()) return it->second;
        auto shard = std::make_unique<KeyShard>(MAX_SHARD_CAPACITY);
        return key_shards_.emplace(key, std::move(shard)).first->second;
    }();

    if (!shard_ptr->enqueue(std::forward<F>(f))) return false;

    stats_.add_queued();
    stats_.add_submitted();

    {
        std::lock_guard<std::mutex> lock(key_mtx_);
        active_keys_.insert(key);
    }
    key_cv_.notify_one();
    return true;
}

}  // namespace ts
