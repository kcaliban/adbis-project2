//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_ExternalSortMultiThreaded_H
#define PROJECT2_ExternalSortMultiThreaded_H

#include "CSVReader.h"
#include <fstream>
#include <filesystem>
#include <unordered_map>

class ExternalSortMultiThreaded {
public:
    ExternalSortMultiThreaded(CSVReader *A, const std::string &column, unsigned long long cacheSize,
                              const std::filesystem::path &tempDir, const std::filesystem::path &outputFilePath,
                              unsigned int numThreads);

    void Sort();
private:
    unsigned long long cacheSize;
    unsigned int columnIndex;
    std::vector<std::vector<std::vector<unsigned long>*>*> caches;
    CSVReader * A;
    std::vector<std::string> tempFiles;
    std::filesystem::path tempDir;
    std::filesystem::path outputFilePath;
    unsigned int numThreads;

    unsigned long long GetCachedSize(unsigned long long);
    void CreateAndSortChunks();
    void KWayMergeSort();
    void KWayMergeSortMem();
    void MergeSort(const std::string& a, const std::string& b, const std::string& outputFile);
};


#endif //PROJECT2_ExternalSortMultiThreaded_H
