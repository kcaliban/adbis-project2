//
// Created by fk on 18.07.22.
//
#include "gtest/gtest.h"
#include "CSVReader.h"
#include "HashJoin.h"
#include <istream>
#include <cstdio>
#include <filesystem>
#include <cmath>
#include <vector>
#include <iostream>

const size_t cacheSize = pow(2, 20);

TEST(hash_join, simple_join) {
    std::istringstream tableAStream(
R"""(A,B
a1,b1
a2,b2)"""
    );
    std::istringstream tableBStream(
R"""(B,C
b1,c1
b2,c2)"""
    );

    auto * tableA = new CSVReader(&tableAStream, ',', "tableA");
    auto * tableB = new CSVReader(&tableBStream, ',', "tableB");

    auto * output = new std::stringstream;

    HashJoin hashJoin = HashJoin(tableA, "B", tableB, "B", output, cacheSize);
    hashJoin.Join(cacheSize);

    EXPECT_EQ(
R"""(A,tableA_tableB,C
a1,b1,c1
a2,b2,c2
)""", output->str());

    delete tableA; delete tableB; delete output;
}

TEST(hash_join, simple_join_r_dropped) {
    std::istringstream tableAStream(
            R"""(A,B
a1,b1
a2,b1)"""
    );
    std::istringstream tableBStream(
            R"""(B,C
b1,c1
b2,c2)"""
    );

    auto * tableA = new CSVReader(&tableAStream, ',', "tableA");
    auto * tableB = new CSVReader(&tableBStream, ',', "tableB");

    auto * output = new std::stringstream;

    HashJoin hashJoin = HashJoin(tableA, "B", tableB, "B", output, cacheSize);
    hashJoin.Join(cacheSize);

    EXPECT_EQ(
            R"""(A,tableA_tableB,C
a1,b1,c1
a2,b1,c1
)""", output->str());

    delete tableA; delete tableB; delete output;
}

TEST(hash_join, simple_join_l_dropped) {
    std::istringstream tableAStream(
            R"""(A,B
a1,b1
a2,b2)"""
    );
    std::istringstream tableBStream(
            R"""(B,C
b1,c1
b1,c2)"""
    );

    auto * tableA = new CSVReader(&tableAStream, ',', "tableA");
    auto * tableB = new CSVReader(&tableBStream, ',', "tableB");

    auto * output = new std::stringstream;

    HashJoin hashJoin = HashJoin(tableA, "B", tableB, "B", output, cacheSize);
    hashJoin.Join(cacheSize);

    EXPECT_EQ(
            R"""(A,tableA_tableB,C
a1,b1,c1
a1,b1,c2
)""", output->str());

    delete tableA; delete tableB; delete output;
}

TEST(hash_join, three_col_join) {
    std::istringstream tableAStream(
            R"""(A,B,C
a1,b1,c1
a2,b1,c1)"""
    );
    std::istringstream tableBStream(
            R"""(C,D
c1,d1
c1,d2)"""
    );

    auto * tableA = new CSVReader(&tableAStream, ',', "tableA");
    auto * tableB = new CSVReader(&tableBStream, ',', "tableB");

    auto * output = new std::stringstream;

    HashJoin hashJoin = HashJoin(tableA, "C", tableB, "C", output, cacheSize);
    hashJoin.Join(cacheSize);

    EXPECT_EQ(
            R"""(A,B,tableA_tableB,D
a1,b1,c1,d1
a2,b1,c1,d1
a1,b1,c1,d2
a2,b1,c1,d2
)""", output->str());

    delete tableA; delete tableB; delete output;
}

TEST(hash_join, join_result_join) {
    std::istringstream tableAStream(
            R"""(A,B
a1,b1
a2,b2)"""
    );
    std::istringstream tableBStream(
            R"""(B,C
b1,c1
b2,c2)"""
    );
    std::istringstream tableCStream(
            R"""(C,D
c1,d1
c2,d2)"""
    );
    auto * tableA = new CSVReader(&tableAStream, ',', "tableA");
    auto * tableB = new CSVReader(&tableBStream, ',', "tableB");
    auto * tableC = new CSVReader(&tableCStream, ',', "tableC");

    auto * output = new std::stringstream;

    HashJoin hashJoin = HashJoin(tableA, "B", tableB, "B", output, cacheSize);
    hashJoin.Join(cacheSize);

    output->seekg(0, std::ios::beg);
    auto * outputReader = new CSVReader(output, ',', "tableA_tableB");

    auto * output2 = new std::stringstream;

    HashJoin hashJoin2 = HashJoin(outputReader, "C", tableC, "C", output2, cacheSize);
    hashJoin2.Join(cacheSize);

    EXPECT_EQ(
            R"""(A,tableA_tableB,tableA_tableB_tableC,D
a1,b1,c1,d1
a2,b2,c2,d2
)""", output2->str());

    delete tableA; delete tableB; delete tableC; delete output; delete outputReader; delete output2;
}