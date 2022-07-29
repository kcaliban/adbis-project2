cmake_minimum_required(VERSION 3.22)
project(project2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

set(SOURCE main.cpp)
add_executable(project2_run ${SOURCE})

include_directories(src)

add_subdirectory(src)

target_link_libraries(project2_run project2 Threads::Threads)
