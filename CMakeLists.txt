cmake_minimum_required(VERSION 3.22)
project(project2)

set(CMAKE_CXX_STANDARD 20)

configure_file(100k.txt 100k.txt COPYONLY)
configure_file(watdiv.10M.nt watdiv.10M.nt COPYONLY)
add_executable(project2 main.cpp Relation.h HashJoin.h)
