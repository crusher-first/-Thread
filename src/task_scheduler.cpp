#include "task_scheduler/task_scheduler.hpp"
#include <exception>

namespace ts {

// ── KeyShard ──────────────────────────────────────────────────────────────────

void ts::KeyShard::process_all(std::function<void()> on_complete) {
    std::function<void()> f;
    while (dequeue(f)) {
        if (f) f();
        on_complete();
    }
}

// ── ctor / dtor ─────────────────────────────────────────────────────────────

TaskScheduler::TaskScheduler(const SchedulerConfig& cfg)
    : config_(cfg),
      task_queue_(config_.queue_capacity),
      running_(true),
      workers_(config_.thread_num) {

    for (auto& t : workers_) {
        t = std::thread([this] { worker_loop(); });
    }
    timer_thread_ = std::thread([this] { timer_loop(); });
    key_thread_  = std::thread([this] { key_serial_loop(); });
}

TaskScheduler::~TaskScheduler() { shutdown(); }

void TaskScheduler::shutdown() noexcept {
    if (stopping_.exchange(true)) return;
    running_.store(false, std::memory_order_release);
    task_queue_.stop();

    // Drain all delayed tasks synchronously before stopping timer thread
    {
        std::lock_guard<std::mutex> lock(delay_mtx_);
        while (!delay_heap_.empty()) {
            auto task = std::move(const_cast<DelayedTask&>(delay_heap_.top()));
            delay_heap_.pop();
            if (task.func) {
                // Run directly — scheduler is shutting down so run inline
                try { task.func(); } catch (const std::exception&) {}
                stats_.add_completed();
            }
        }
    }

    delay_cv_.notify_all();
    key_cv_.notify_all();

    if (timer_thread_.joinable()) timer_thread_.join();
    if (key_thread_.joinable())   key_thread_.join();
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
}

// ── worker loop ─────────────────────────────────────────────────────────────

void TaskScheduler::worker_loop() {
    while (true) {
        auto node = task_queue_.wait_pop();
        if (!node) break;   // stopped
        if (node->func) {
            try {
                node->func();
            } catch (const std::exception&) {}
            stats_.add_completed();
            stats_.sub_queued();
        }
    }
}

// ── timer loop ───────────────────────────────────────────────────────────────

void TaskScheduler::timer_loop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> lock(delay_mtx_);

        if (delay_heap_.empty()) {
            delay_cv_.wait_for(lock, std::chrono::seconds(1), [this] {
                return stopping_.load(std::memory_order_acquire) || !delay_heap_.empty();
            });
            if (stopping_.load(std::memory_order_acquire)) break;
            continue;
        }

        auto now = std::chrono::steady_clock::now();
        auto top_time = delay_heap_.top().run_time;
        if (top_time > now) {
            // Sleep until the earliest task is ready. The predicate must only
            // return true when there is actual work to do (top task's run_time
            // has arrived) or we are stopping — otherwise wait_for returns
            // immediately because the heap is non-empty, causing a busy-loop.
            delay_cv_.wait_until(lock, top_time, [this] {
                if (stopping_.load(std::memory_order_acquire)) return true;
                if (delay_heap_.empty()) return true;
                return delay_heap_.top().run_time <= std::chrono::steady_clock::now();
            });
            if (stopping_.load(std::memory_order_acquire)) break;
            continue;
        }

        auto task = std::move(const_cast<DelayedTask&>(delay_heap_.top()));
        delay_heap_.pop();
        lock.unlock();

        if (task.func) {
            auto fn = std::move(task.func);
            try { if (fn) fn(); } catch (const std::exception&) {}
        }
    }
}

// ── key-serial loop ──────────────────────────────────────────────────────────

void TaskScheduler::key_serial_loop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        std::string key;

        {
            std::unique_lock<std::mutex> lock(key_mtx_);
            key_cv_.wait_for(lock, std::chrono::milliseconds(500), [this] {
                return stopping_.load(std::memory_order_acquire) || !active_keys_.empty();
            });
            if (stopping_.load(std::memory_order_acquire)) break;
            if (active_keys_.empty()) continue;
            auto it = active_keys_.begin();
            key = *it;
            active_keys_.erase(it);
        }

        // Process this shard; if more work arrives it will re-activate the key
        KeyShard* shard = nullptr;
        {
            std::lock_guard<std::mutex> lock(key_mtx_);
            auto it = key_shards_.find(key);
            if (it != key_shards_.end()) shard = it->second.get();
        }

        if (shard) {
            shard->process_all([&] { stats_.sub_queued_and_serial_completed(); });

            // If new work arrived while processing, re-mark active
            if (shard->has_pending()) {
                std::lock_guard<std::mutex> lock(key_mtx_);
                active_keys_.insert(key);
                key_cv_.notify_one();
            }
        }
    }
}

// ── await ────────────────────────────────────────────────────────────────────

void TaskScheduler::await_until_empty() const {
    while (true) {
        if (stats_.submitted_count() == stats_.completed_count()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace ts
