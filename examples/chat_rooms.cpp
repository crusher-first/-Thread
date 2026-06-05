#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include "task_scheduler/task_scheduler.hpp"

int main() {
    std::cout << "=== Chat Room Simulation (key-serial) ===\n\n";

    ts::SchedulerConfig cfg;
    cfg.thread_num = 4;
    cfg.queue_capacity = 512;
    ts::TaskScheduler sch(cfg);

    // Simulate 3 chat rooms, each with ordered messages
    std::vector<std::string> rooms = {"room-alpha", "room-beta", "room-gamma"};

    for (int round = 0; round < 3; ++round) {
        for (auto& room : rooms) {
            sch.submit_serial(room, [&, r = round, &room_name = room] {
                std::cout << "  [round " << r << "] " << room_name
                          << " processed on thread " << std::this_thread::get_id() << "\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            });
        }
    }

    sch.await_until_empty();
    std::cout << "\n✓ All room messages processed in order\n";

    // Stats
    const auto& st = sch.stats();
    std::cout << "  submitted=" << st.submitted_count()
              << " completed=" << st.completed_count() << "\n";

    return 0;
}
