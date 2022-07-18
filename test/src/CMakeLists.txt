include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

add_executable(runProject2Tests
        HashJoinTests.cpp
)

target_link_libraries(runProject2Tests gtest gtest_main)
target_link_libraries(runProject2Tests project2)