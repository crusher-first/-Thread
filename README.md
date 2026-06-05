# task-scheduler

> 轻量级 C++17 并发任务调度器

## 功能特性

| 特性 | 说明 |
|------|------|
| **固定线程池** | 基于 `std::jthread`，自动管理生命周期 |
| **异步返回值** | `submit_with_result()` + `packaged_task`，异常自动穿透 |
| **MPMC 有界队列** | 生产者/消费者多线程安全，消除忙等 |
| **优先级调度** | 高优先级任务优先执行，同优先级 FIFO |
| **背压控制** | `BLOCK` / `TIMEOUT` / `REJECT` 三种提交策略 |
| **延迟调度** | 小根堆独立线程，毫秒级精度，无轮询开销 |
| **按 Key 串行** | 哈希分片，保证同 Key 任务保序执行 |
| **运行统计** | submitted / completed / rejected / queued 实时计数 |

## 快速开始

### 构建

```bash
git clone https://github.com/crusher-first/-Thread.git
cd -Thread
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

### 运行示例

```bash
./examples/chat_rooms      # 聊天室场景演示
./examples/benchmark 4 100000  # 吞吐量压测
```

### 运行测试

```bash
cd build
ctest --output-on-failure
```

## API 概览

```cpp
#include "task_scheduler/task_scheduler.hpp"

ts::SchedulerConfig cfg;
cfg.thread_num = 4;
cfg.queue_capacity = 1024;
ts::TaskScheduler sch(cfg);

// 普通提交
sch.submit(ts::TaskPriority::HIGH, [] { do_work(); });

// 获取返回值
auto fut = sch.submit_with_result(ts::TaskPriority::NORMAL,
    [](int a, int b) { return a + b; }, 3, 7);
int result = fut.get();  // 10

// 延迟调度（100ms 后执行）
sch.schedule_after(ts::TaskPriority::LOW, std::chrono::milliseconds(100), [] {
    std::cout << "delayed!\n";
});

// 按 Key 串行（聊天室保序）
sch.submit_serial("room-1", [] { process_message(); });

// 统计
auto st = sch.stats();
std::cout << st.completed_count() << " tasks done\n";

// 等待所有任务完成
sch.await_until_empty();
```

## 项目结构

```
.
├── include/
│   └── task_scheduler/
│       └── task_scheduler.hpp   # 头文件（全部模板实现）
├── src/
│   ├── CMakeLists.txt
│   └── task_scheduler.cpp       # 核心实现
├── test/
│   ├── CMakeLists.txt
│   └── test_task_scheduler.cpp  # 单元测试
├── examples/
│   ├── CMakeLists.txt
│   ├── chat_rooms.cpp           # 聊天室演示
│   └── benchmark.cpp            # 吞吐量压测
├── CMakeLists.txt
└── README.md
```

## 架构设计

```
┌─────────────────────────────────────────────┐
│              TaskScheduler                   │
│  ┌──────────────────────────────────────┐  │
│  │   BoundedQueue<TaskNode> (MPMC)       │  │
│  │   + std::priority_queue (外部)         │  │
│  └──────────────┬───────────────────────┘  │
│       ┌─────────┼──────────┐                │
│       ▼         ▼          ▼                │
│  [Worker 0] [Worker 1] ... [Worker N]       │
│                                              │
│  ┌──────────────┐  ┌────────────────────┐  │
│  │ Timer Thread  │  │ Key Serial Thread  │  │
│  │ (delay heap)  │  │ (per-key shards)   │  │
│  └──────────────┘  └────────────────────┘  │
└─────────────────────────────────────────────┘
```

## 性能

- 单机 4 线程，10 万任务压测稳定
- 有界队列 + 条件变量，消除 CPU 空转
- 无锁分片 + 独立定时线程，减少锁竞争
