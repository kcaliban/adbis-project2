//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_HASHJOIN_H
#define PROJECT2_HASHJOIN_H

#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <array>
#include "CSVReader.h"
#include "CSVWriter.h"

class HashJoin {
public:
    explicit HashJoin(CSVReader * A, const std::string& columnA,
                      CSVReader * B, const std::string& columnB,
                      std::ostream * ostream, unsigned long long cacheSize,
                      unsigned long long hashTableSize);
    void Join();
    virtual ~HashJoin() {
        delete output;
    }
private:
    unsigned int columnAIndex;
    unsigned int columnBIndex;
    std::string columnA;
    std::string columnB;
    std::ostream * ostream;
    unsigned long long cacheSize;
    unsigned long long hashTableSize;
    CSVReader * A;
    CSVReader * B;
    CSVWriter * output;

    std::unordered_map<std::string, std::vector<std::vector<std::string>>> hashMap;

    void InitializeIndices();
    void Probe();
    void InitializeOutput();
    unsigned long long GetHashTableSize();
};

#endif //PROJECT2_HASHJOIN_H
