//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_HASHJOIN_H
#define PROJECT2_HASHJOIN_H

#include "Relation.h"
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
    explicit HashJoin() = default;
    void Join(CSVReader * A, const std::string& columnA,
              CSVReader * B, const std::string& columnB,
              const std::string& outputFile, unsigned int cacheSize);
    void Reset();
private:
    unsigned int columnAIndex;
    unsigned int columnBIndex;
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> hashMap;

    void InitializeIndices(CSVReader * A, CSVReader * B,
                           const std::string & columnA, const std::string & columnB);

    void Build(CSVReader *A);

    void Probe(CSVReader * A, CSVReader * B, const std::string& column, const std::string& filename, unsigned int cacheSize);
};

#endif //PROJECT2_HASHJOIN_H
