//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_EXTERNALSORT_H
#define PROJECT2_EXTERNALSORT_H

#include "CSVReader.h"
#include <fstream>
#include <filesystem>
#include <unordered_map>

class ExternalSort {
public:
    ExternalSort(CSVReader *A, const std::string &column, size_t cacheSize, const std::filesystem::path &tempDir,
                 const std::filesystem::path &outputFilePath);

    void Sort();
private:
    size_t cacheSize;
    unsigned int columnIndex;
    std::vector<std::vector<std::string>> cache;
    CSVReader * A;
    std::vector<std::string> tempFiles;
    std::filesystem::path tempDir;
    std::filesystem::path outputFilePath;

    unsigned int GetCachedSize();
    void WriteCacheToNewFile();
    void CreateAndSortChunks();
    void KWayMergeSort();
    void MergeSort(const std::string& a, const std::string& b, const std::string& outputFile);
    std::string GetRandomString();
};


#endif //PROJECT2_EXTERNALSORT_H
