add_executable(chat_rooms chat_rooms.cpp)
target_link_libraries(chat_rooms PRIVATE task_scheduler)

add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark PRIVATE task_scheduler)
