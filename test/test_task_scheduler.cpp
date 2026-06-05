#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <numeric>
#include <thread>
#include "task_scheduler/task_scheduler.hpp"

using namespace ts;

// ── basic smoke ─────────────────────────────────────────────────────────────

TEST(TaskScheduler, SubmitAndWait) {
    SchedulerConfig cfg;
    cfg.thread_num = 2;
    cfg.queue_capacity = 16;
    TaskScheduler sch(cfg);

    int x = 0;
    bool ok = sch.submit(TaskPriority::NORMAL, [&] { ++x; });
    EXPECT_TRUE(ok);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(x, 1);
}

// ── priority order ───────────────────────────────────────────────────────────

TEST(TaskScheduler, PriorityOrdering) {
    SchedulerConfig cfg;
    cfg.thread_num = 1;
    cfg.queue_capacity = 64;
    TaskScheduler sch(cfg);

    std::vector<int> order;
    std::mutex m;

    for (int i = 0; i < 6; ++i) sch.submit(TaskPriority::LOW,  [&, i] { std::lock_guard l(m); order.push_back(i); });
    for (int i = 6; i < 12; ++i) sch.submit(TaskPriority::HIGH, [&, i] { std::lock_guard l(m); order.push_back(i); });

    sch.await_until_empty();

    // all HIGH (6..11) should appear before LOW (0..5)
    auto hi_it = std::find_if(order.begin(), order.end(), [](int v) { return v >= 6; });
    auto lo_it = std::find_if(order.begin(), order.end(), [](int v) { return v < 6; });
    ASSERT_NE(hi_it, order.end());
    ASSERT_NE(lo_it, order.end());
    EXPECT_LT(std::distance(order.begin(), hi_it), std::distance(order.begin(), lo_it));
}

// ── future result ────────────────────────────────────────────────────────────

TEST(TaskScheduler, FutureResult) {
    SchedulerConfig cfg;
    cfg.thread_num = 2;
    TaskScheduler sch(cfg);

    auto fut = sch.submit_with_result(TaskPriority::NORMAL,
        [](int a, int b) { return a + b; }, 3, 7);

    EXPECT_EQ(fut.get(), 10);
}

// ── exception propagation ────────────────────────────────────────────────────

TEST(TaskScheduler, ExceptionPropagation) {
    SchedulerConfig cfg;
    cfg.thread_num = 1;
    TaskScheduler sch(cfg);

    auto fut = sch.submit_with_result(TaskPriority::NORMAL, [] {
        throw std::runtime_error("test error");
        return 0;
    });

    EXPECT_THROW(fut.get(), std::runtime_error);
}

// ── delay schedule ──────────────────────────────────────────────────────────

TEST(TaskScheduler, ScheduleAfter) {
    SchedulerConfig cfg;
    cfg.thread_num = 2;
    TaskScheduler sch(cfg);

    int x = 0;
    auto start = std::chrono::steady_clock::now();
    sch.schedule_after(TaskPriority::NORMAL, std::chrono::milliseconds(80), [&] { ++x; });

    sch.await_until_empty();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(x, 1);
    EXPECT_GE(elapsed, std::chrono::milliseconds(70));
}

// ── serial by key ─────────────────────────────────────────────────────────────

TEST(TaskScheduler, SerialByKey) {
    SchedulerConfig cfg;
    cfg.thread_num = 4;
    cfg.queue_capacity = 64;
    TaskScheduler sch(cfg);

    std::vector<int> seq_a, seq_b;
    std::mutex ma, mb;

    for (int i = 0; i < 5; ++i) {
        sch.submit_serial("room-A", [&, i] {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            std::lock_guard l(ma); seq_a.push_back(i);
        });
        sch.submit_serial("room-B", [&, i] {
            std::lock_guard l(mb); seq_b.push_back(i);
        });
    }

    sch.await_until_empty();

    // each room's tasks should be in order
    EXPECT_EQ(seq_a, (std::vector<int>{0,1,2,3,4}));
    EXPECT_EQ(seq_b, (std::vector<int>{0,1,2,3,4}));
}

// ── back-pressure (reject) ───────────────────────────────────────────────────

TEST(TaskScheduler, RejectPolicy) {
    SchedulerConfig cfg;
    cfg.thread_num = 1;
    cfg.queue_capacity = 2;
    cfg.submit_policy = SubmitPolicy::REJECT;
    TaskScheduler sch(cfg);

    std::atomic<int> done{0};
    // first 2 succeed
    EXPECT_TRUE(sch.submit(TaskPriority::NORMAL, [&] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); ++done; }));
    EXPECT_TRUE(sch.submit(TaskPriority::NORMAL, [&] { ++done; }));

    // queue is full, should reject
    bool ok3 = sch.submit(TaskPriority::NORMAL, [&] { ++done; });
    EXPECT_FALSE(ok3);

    const auto& st = sch.stats();
    EXPECT_EQ(st.rejected_count(), 1u);
}

// ── stats ────────────────────────────────────────────────────────────────────

TEST(TaskScheduler, Stats) {
    SchedulerConfig cfg;
    cfg.thread_num = 2;
    TaskScheduler sch(cfg);

    sch.submit(TaskPriority::HIGH, [] {});
    sch.submit(TaskPriority::LOW,  [] {});
    sch.submit(TaskPriority::NORMAL, [] {});

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto& st = sch.stats();
    EXPECT_EQ(st.submitted_count(), 3u);
    EXPECT_EQ(st.completed_count(), 3u);
}

// ── throughput (quick sanity) ─────────────────────────────────────────────────

TEST(TaskScheduler, Throughput) {
    SchedulerConfig cfg;
    cfg.thread_num = 4;
    cfg.queue_capacity = 1024;
    TaskScheduler sch(cfg);

    constexpr int N = 100000;
    std::atomic<int> counter{0};

    for (int i = 0; i < N; ++i) {
        sch.submit(TaskPriority::NORMAL, [&] { ++counter; });
    }

    sch.await_until_empty();
    EXPECT_EQ(counter.load(), N);
}

// ── lifecycle ─────────────────────────────────────────────────────────────────

TEST(TaskScheduler, ShutdownDestructor) {
    SchedulerConfig cfg;
    cfg.thread_num = 2;
    {
        TaskScheduler sch(cfg);
        sch.submit(TaskPriority::NORMAL, [] {});
    } // dtor runs shutdown
    // no crash = pass
}
