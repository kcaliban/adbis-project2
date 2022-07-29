//
// Created by fk on 21.07.22.
//

#include "ExternalSort.h"
#include "CSVWriter.h"
#include <algorithm>
#include <random>
#include <iostream>
#include <thread>
#include <future>
#include <chrono>
#include "Auxiliary.h"

/* CLASS FUNCTIONS */
ExternalSort::ExternalSort(CSVReader *A, const std::string & column,
                           unsigned long long cacheSize, const std::filesystem::path & tempDir,
                           const std::filesystem::path & outputFilePath) {
    this->A = A;
    this->cacheSize = cacheSize;
    this->tempDir = tempDir;
    this->outputFilePath = outputFilePath;

    for (int i = 0; i < A->columnNames.size(); i++) {
        if (A->columnNames[i] == column)
            columnIndex = i;
    }
}

void ExternalSort::Sort(unsigned int numThreads) {
    std::cout << "\t\t\tCREATE AND SORT CHUNKS" << std::endl;
    auto startChunks = std::chrono::steady_clock::now();
    CreateAndSortChunks();
    auto endChunks = std::chrono::steady_clock::now();
    std::cout << "\t\t\tFINISHED IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endChunks - startChunks).count() << "[ms]" << std::endl;

    std::cout << "\t\t\tMERGE CHUNKS" << std::endl;
    auto startMerge = std::chrono::steady_clock::now();
    KWayMergeSort();
    auto endMerge = std::chrono::steady_clock::now();
    std::cout << "\t\t\tFINISHED IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endMerge - startMerge).count() << "[ms]" << std::endl;
}

void ExternalSort::ClearCache() {
    for (auto it: cache) {
        delete it;
    }
    cache.clear();
}

void ExternalSort::SortCache() {
    std::sort(cache.begin(), cache.end(),
              [this](std::vector<unsigned long> * a,
                     std::vector<unsigned long> * b) {
                  return (*a)[columnIndex] < (*b)[columnIndex];
    });
}

void ExternalSort::CreateAndSortChunks() {
    auto row = A->GetNextRow();

    while (!row.empty()) {
        auto newRowPtr = new std::vector<unsigned long>(row);
        if (GetCachedSize() < cacheSize) {
            cache.push_back(newRowPtr);
        } else {
            cache.push_back(newRowPtr);
            SortCache();
            WriteCacheToNewFile();
            ClearCache();
        }
        row = A->GetNextRow();
    }

    if (!cache.empty()) {
        SortCache();
        WriteCacheToNewFile();
        ClearCache();
    }

    // Clear memory
    cache.shrink_to_fit();
}

unsigned long long ExternalSort::GetCachedSize() {
    if (cache.empty()) return 0;
    return GetCachedSize(cache.size());
}

unsigned long long ExternalSort::GetCachedSize(unsigned long long elements) {
    unsigned long long size = sizeof(std::vector<std::vector<unsigned long>*>) // Size of vector
                              + ((unsigned long long) round(elements * 1.3))
                                * sizeof(std::vector<unsigned long>*) // Size of pointers
                              + ((unsigned long long) round(elements * 1.3))
                                * sizeof(std::vector<unsigned long>) // Size of actual vectors
                              + ((unsigned long long) round(elements * 1.3))
                                * sizeof(unsigned long) * A->columnNames.size(); // Size of rows
    return size;
}

void ExternalSort::WriteCacheToNewFile() {
    std::string outputFileName = GetRandomString();
    std::filesystem::path output = tempDir / outputFileName;
    std::ofstream ofstream(output);

    CSVWriter writer(&ofstream, A->columnNames, ',');
    for (const auto row : cache) {
        writer.WriteNextRow(*row);
    }

    tempFiles.push_back(outputFileName);
}

void ExternalSort::KWayMergeSort() {
    while (tempFiles.size() > 1) {
        std::vector<std::string> newTempFiles;
        std::vector<std::pair<std::string,std::string>> mergePairs;
        for (int i = 0; i < tempFiles.size() - 1; i += 2) {
            mergePairs.emplace_back(
                    std::make_pair(
                            tempFiles[i], tempFiles[i + 1]
                    )

            );
        }

        // Merge pairs into new files
        for (const auto & pair : mergePairs) {
            auto mergedFileName = GetRandomString();
            auto mergedFilePath = tempDir / mergedFileName;


            MergeSort(pair.first, pair.second, mergedFilePath);
            newTempFiles.push_back(mergedFileName);
        }

        // If number is uneven, merge last merge result with remaining file
        if (tempFiles.size() % 2 != 0) {
            auto mergedFileName = GetRandomString();
            auto mergedFilePath = tempDir / mergedFileName;

            MergeSort(newTempFiles.back(), tempFiles.back(), mergedFilePath);

            auto tmpFile = newTempFiles.back();
            newTempFiles.pop_back();

            std::filesystem::path filePath = tempDir / tmpFile;
            std::filesystem::remove(filePath);

            newTempFiles.emplace_back(mergedFileName);
        }

        for (const auto & tmpFile : tempFiles) {
            std::filesystem::path filePath = tempDir / tmpFile;
            std::filesystem::remove(filePath);
        }

        // Set tempFiles = newTempFiles
        tempFiles = newTempFiles;
    }

    // Move file
    std::filesystem::rename(tempDir / tempFiles[0], outputFilePath);
}

void ExternalSort::MergeSort(const std::string& a, const std::string& b, const std::string& outputFile) {
    std::ofstream ostream(outputFile);
    std::ifstream istreamA(tempDir / a);
    std::ifstream istreamB(tempDir / b);

    CSVReader readerA(&istreamA, ',', "a");
    CSVReader readerB(&istreamB, ',', "b");
    CSVWriter writer(&ostream, A->columnNames, ',');

    auto rowA = readerA.GetNextRow();
    auto rowB = readerB.GetNextRow();

    while (!rowA.empty() && !rowB.empty()) {
        if (rowA[columnIndex] < rowB[columnIndex]) {
            writer.WriteNextRow(rowA);
            rowA = readerA.GetNextRow();
        } else if (rowA[columnIndex] > rowB[columnIndex]) {
            writer.WriteNextRow(rowB);
            rowB = readerB.GetNextRow();
        } else {
            writer.WriteNextRow(rowA);
            writer.WriteNextRow(rowB);
            rowA = readerA.GetNextRow();
            rowB = readerB.GetNextRow();
        }
    }

    // Write remaining rows
    while (!rowA.empty()) {
        writer.WriteNextRow(rowA);
        rowA = readerA.GetNextRow();
    }
    while (!rowB.empty()) {
        writer.WriteNextRow(rowB);
        rowB = readerB.GetNextRow();
    }
}


