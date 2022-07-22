//
// Created by fk on 21.07.22.
//

#include "ExternalSort.h"
#include "CSVWriter.h"
#include <algorithm>
#include <random>
#include <iostream>

ExternalSort::ExternalSort(CSVReader *A, const std::string & column,
                           size_t cacheSize, const std::filesystem::path & tempDir,
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

void ExternalSort::Sort() {
    CreateAndSortChunks();
    KWayMergeSort();
}

void ExternalSort::CreateAndSortChunks() {
    auto row = A->GetNextRow();

    while (!row.empty()) {
        if (GetCachedSize() < cacheSize) {
            cache.push_back(row);
        } else {
            cache.push_back(row);
            std::sort(cache.begin(), cache.end(),
                      [this](std::vector<std::string> a,
                          std::vector<std::string> b) {
                          return a[columnIndex] < b[columnIndex];
                      });

            WriteCacheToNewFile();
            cache.clear();
        }
        row = A->GetNextRow();
    }

    if (!cache.empty()) {
        std::sort(cache.begin(), cache.end(),
                  [this](std::vector<std::string> a,
                         std::vector<std::string> b) {
                      return a[columnIndex] < b[columnIndex];
        });
        WriteCacheToNewFile();
        cache.clear();
    }
    cache.shrink_to_fit();
}

unsigned int ExternalSort::GetCachedSize() {
    if (cache.empty()) return 0;
    // This is just an approximation. String size varies per row and column entry.
    size_t size = sizeof(cache) + cache.size() * (
            sizeof(cache[0]) + cache[0].size() * (
                    sizeof(cache[0][0]) + cache[0][0].length()
            )
    );

    return size;
}

void ExternalSort::WriteCacheToNewFile() {
    std::string outputFileName = GetRandomString();
    std::filesystem::path output = tempDir / outputFileName;
    std::ofstream ofstream(output);

    CSVWriter writer(&ofstream, A->columnNames, ',', cacheSize);
    for (const auto & row : cache) {
        writer.WriteNextRow(row);
    }
    writer.FlushCache();

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
    CSVWriter writer(&ostream, A->columnNames, ',', cacheSize);

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

    // Flush
    writer.FlushCache();
}

std::string ExternalSort::GetRandomString() {
    static auto& chrs = "0123456789"
                        "abcdefghijklmnopqrstuvwxyz"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

    std::string s;

    size_t length = 30;
    s.reserve(length);

    while(--length)
        s += chrs[pick(rg)];

    return s;
}
