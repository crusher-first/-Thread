cmake_minimum_required(VERSION 3.16)
project(task_scheduler VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TS_BUILD_TESTS "Build tests" ON)
option(TS_BUILD_EXAMPLES "Build examples" ON)

add_subdirectory(src)

if(TS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

if(TS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
