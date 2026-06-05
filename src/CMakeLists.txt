add_library(task_scheduler STATIC
    task_scheduler.cpp
)

target_include_directories(task_scheduler PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(task_scheduler PUBLIC cxx_std_17)
