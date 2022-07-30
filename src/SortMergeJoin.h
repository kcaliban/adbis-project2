//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_SORTMERGEJOIN_H
#define PROJECT2_SORTMERGEJOIN_H

#include "CSVReader.h"
#include "CSVWriter.h"
#include <filesystem>
#include <set>

class SortMergeJoin {
public:
    explicit SortMergeJoin(CSVReader * A, const std::string& columnA,
                           CSVReader * B, const std::string& columnB,
                           std::ostream * ostream, unsigned long long cacheSize,
                           const std::filesystem::path & tempDir);
    void Join(bool multiThreaded = false);
    virtual ~SortMergeJoin() {
        delete output;
        delete sortedA;
        delete sortedB;
        delete istreamA;
        delete istreamB;
    };
private:
    unsigned int columnAIndex;
    unsigned int columnBIndex;
    std::string columnA;
    std::string columnB;
    CSVReader * A;
    CSVReader * B;
    CSVReader * sortedA;
    CSVReader * sortedB;
    CSVWriter * output;
    std::ostream * ostream;
    std::ifstream * istreamA;
    std::ifstream * istreamB;
    unsigned long long cacheSize;
    std::filesystem::path tempDir;

    void Sort(bool multiThreaded = false);
    void InitializeOutput();
    void Merge();
    std::pair<std::vector<std::vector<unsigned long>>,std::vector<unsigned long>> Collect(CSVReader *reader, const std::vector<unsigned long> &row, unsigned int columnIndex);
    void
    CartesianProduct(const std::vector<std::vector<unsigned long>> & a, const std::vector<std::vector<unsigned long>> & b);
};


#endif //PROJECT2_SORTMERGEJOIN_H
