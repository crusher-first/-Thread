#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include "task_scheduler/task_scheduler.hpp"

int main(int argc, char* argv[]) {
    int threads = (argc > 1) ? std::stoi(argv[1]) : 4;
    int tasks    = (argc > 2) ? std::stoi(argv[2]) : 100000;

    std::cout << "=== Throughput Benchmark ===\n";
    std::cout << "threads=" << threads << "  tasks=" << tasks << "\n\n";

    ts::SchedulerConfig cfg;
    cfg.thread_num = static_cast<size_t>(threads);
    cfg.queue_capacity = 8192;
    ts::TaskScheduler sch(cfg);

    std::atomic<int> counter{0};

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < tasks; ++i) {
        sch.submit(ts::TaskPriority::NORMAL, [&] { ++counter; });
    }

    sch.await_until_empty();

    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    auto ms = std::chrono::duration<double, std::milli>(elapsed).count();

    const auto& st = sch.stats();
    std::cout << "elapsed: " << ms << " ms\n";
    std::cout << "throughput: " << (tasks / (ms / 1000.0)) << " tasks/sec\n";
    std::cout << "stats: submitted=" << st.submitted_count()
              << " completed=" << st.completed_count()
              << " rejected=" << st.rejected_count() << "\n";

    return 0;
}
