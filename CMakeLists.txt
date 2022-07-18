cmake_minimum_required(VERSION 3.22)
project(project2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(SOURCE main.cpp)
add_executable(project2_run ${SOURCE})

include_directories(src)

add_subdirectory(src)
add_subdirectory(test)

target_link_libraries(project2_run project2)
