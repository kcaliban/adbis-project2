cmake_minimum_required(VERSION 3.22)
project(project2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

configure_file(100k.txt 100k.txt COPYONLY)
configure_file(watdiv.10M.nt watdiv.10M.nt COPYONLY)
add_executable(project2 main.cpp HashJoin.h CSVReader.cpp CSVReader.h CSVWriter.cpp CSVWriter.h HashJoin.cpp)
