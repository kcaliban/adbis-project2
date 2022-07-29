//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_ExternalSort_H
#define PROJECT2_ExternalSort_H

#include "CSVReader.h"
#include <fstream>
#include <filesystem>
#include <unordered_map>

class ExternalSort {
public:
    ExternalSort(CSVReader *A, const std::string &column, unsigned long long cacheSize,
                 const std::filesystem::path &tempDir,
                 const std::filesystem::path &outputFilePath);

    void Sort(unsigned int numThreads = 0);
private:
    unsigned long long cacheSize;
    unsigned int columnIndex;
    std::vector<std::vector<unsigned long>*> cache;
    CSVReader * A;
    std::vector<std::string> tempFiles;
    std::filesystem::path tempDir;
    std::filesystem::path outputFilePath;

    unsigned long long GetCachedSize();
    unsigned long long GetCachedSize(unsigned long long);
    void WriteCacheToNewFile();
    void CreateAndSortChunks();
    void KWayMergeSort();
    void MergeSort(const std::string& a, const std::string& b, const std::string& outputFile);
    void ClearCache();
    void SortCache();
};


#endif //PROJECT2_ExternalSort_H
