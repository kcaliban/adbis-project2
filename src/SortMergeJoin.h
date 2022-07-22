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
                           std::ostream * ostream, unsigned int cacheSize,
                           std::filesystem::path tempDir);
    void Join();
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
    std::istream * istreamA;
    std::istream * istreamB;
    unsigned int cacheSize;
    std::filesystem::path tempDir;

    void Sort();
    void InitializeOutput();
    void Merge();
    std::pair<std::vector<std::vector<std::string>>,std::vector<std::string>> Collect(CSVReader *reader, const std::vector<std::string> &row, unsigned int columnIndex);
    void
    CartesianProduct(const std::vector<std::vector<std::string>> & a, const std::vector<std::vector<std::string>> & b);
};


#endif //PROJECT2_SORTMERGEJOIN_H
