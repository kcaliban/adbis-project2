//
// Created by fk on 21.07.22.
//

#include "SortMergeJoin.h"
#include "ExternalSort.h"
#include "ExternalSortMultiThreaded.h"
#include <unordered_set>
#include <iostream>
#include <thread>
#include <chrono>
#include <cmath>

SortMergeJoin::SortMergeJoin(CSVReader *A, const std::string &columnA, CSVReader *B,
                             const std::string &columnB, std::ostream *ostream,
                             unsigned long long cacheSize,
                             std::filesystem::path tempDir) {
    this->A = A;
    this->B = B;
    this->columnA = columnA;
    this->columnB = columnB;
    this->ostream = ostream;
    this->cacheSize = cacheSize;
    this->tempDir = tempDir;

    for (int i = 0; i < A->columnNames.size(); i++) {
        if (A->columnNames[i] == columnA)
            columnAIndex = i;
    }

    for (int i = 0; i < B->columnNames.size(); i++) {
        if (B->columnNames[i] == columnB)
            columnBIndex = i;
    }

    InitializeOutput();
}

void SortMergeJoin::InitializeOutput() {
    // Initialize output
    auto const & columnNamesA = A->columnNames;
    auto const & columnNamesB = B->columnNames;
    std::vector<std::string> columns;

    // Left over column names from A, Name from A + Name from B, Left over column names from B
    for (int i = 0; i < columnNamesA.size(); i++) {
        if (i != columnAIndex) {
            // If name already contains the name of a table, don't add another one
            std::string tmp = columnNamesA[i];
            if (tmp.find('.') != std::string::npos)
                columns.push_back(columnNamesA[i]);
            else
                columns.push_back(A->tableName + "." + columnNamesA[i]);
        }
    }

    if (A->columnNames[columnAIndex].find('.') != std::string::npos)
        columns.push_back(A->columnNames[columnAIndex]);
    else
        columns.push_back(A->tableName + "." + A->columnNames[columnAIndex]);

    for (auto const & col : columnNamesB) {
        if (col != columnB) {
            // If name already contains the name of a table, don't add another one
            if (col.find('.') != std::string::npos)
                columns.push_back(col);
            else
                columns.push_back(B->tableName + "." + col);
        }
    }

    this->output = new CSVWriter(ostream, columns, ',');
}

void SortMergeJoin::Sort(bool multiThreaded) {
    std::string sortedAFileName = A->tableName + "_sorted";
    std::filesystem::path sortedAOutputPath = tempDir / sortedAFileName;
    std::chrono::time_point<std::chrono::steady_clock> startSortA;
    std::chrono::time_point<std::chrono::steady_clock> endSortA;
    std::cout << "\t\tSTART SORTING A" << std::endl;
    if (multiThreaded) {
        ExternalSortMultiThreaded ExternalSortA(A, columnA, cacheSize, tempDir, sortedAOutputPath, std::thread::hardware_concurrency());
        startSortA = std::chrono::steady_clock::now();
        ExternalSortA.Sort();
        endSortA = std::chrono::steady_clock::now();
    } else {
        ExternalSort ExternalSortA(A, columnA, cacheSize, tempDir, sortedAOutputPath);
        startSortA = std::chrono::steady_clock::now();
        ExternalSortA.Sort();
        endSortA = std::chrono::steady_clock::now();
    }
    istreamA = new std::ifstream(sortedAOutputPath);
    sortedA = new CSVReader(istreamA, ',', sortedAFileName);

    std::cout << "\t\tSORTED A IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortA - startSortA).count() << "[ms]" << std::endl;

    std::string sortedBFileName = B->tableName + "_sorted";
    std::filesystem::path sortedBOutputPath = tempDir / sortedBFileName;
    std::chrono::time_point<std::chrono::steady_clock> startSortB;
    std::chrono::time_point<std::chrono::steady_clock> endSortB;
    std::cout << "\t\tSTART SORTING B" << std::endl;
    if (multiThreaded) {
        ExternalSortMultiThreaded ExternalSortB(B, columnB, cacheSize, tempDir, sortedBOutputPath, std::thread::hardware_concurrency());
        startSortB = std::chrono::steady_clock::now();
        ExternalSortB.Sort();
        endSortB = std::chrono::steady_clock::now();
    } else {
        ExternalSort ExternalSortB(B, columnB, cacheSize, tempDir, sortedBOutputPath);
        startSortB = std::chrono::steady_clock::now();
        ExternalSortB.Sort();
        endSortB = std::chrono::steady_clock::now();
    }
    istreamB = new std::ifstream(sortedBOutputPath);
    sortedB = new CSVReader(istreamB, ',', sortedBFileName);

    std::cout << "\t\tSORTED B IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortB - startSortB).count() << "[ms]" << std::endl;
}

void SortMergeJoin::Join(bool multiThreaded) {
    std::cout << "\tSORT" << std::endl;
    Sort(multiThreaded);
    std::cout << "\tMERGE" << std::endl;
    Merge();
    std::cout << "\tROWS WRITTEN: " << output->getRowsWritten() << std::endl;
}

void SortMergeJoin::Merge() {
    auto startMerge = std::chrono::steady_clock::now();
    auto rowA = sortedA->GetNextRow();
    auto rowB = sortedB->GetNextRow();

    while (!rowA.empty() && !rowB.empty()) {
        if (rowA[columnAIndex] < rowB[columnBIndex]) {
            rowA = sortedA->GetNextRow();
        } else if (rowA[columnAIndex] > rowB[columnBIndex]) {
            rowB = sortedB->GetNextRow();
        } else {
            auto setA = Collect(sortedA, rowA, columnAIndex);
            auto setB = Collect(sortedB, rowB, columnBIndex);
            CartesianProduct(setA.first, setB.first);
            rowA = setA.second;
            rowB = setB.second;
        }
    }
    auto endMerge = std::chrono::steady_clock::now();
    std::cout << "\t\tMERGED IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endMerge - startMerge).count() << "[ms]" << std::endl;
}

std::pair<std::vector<std::vector<unsigned long>>,std::vector<unsigned long>>
SortMergeJoin::Collect(CSVReader * reader,
                       const std::vector<unsigned long> & row,
                       unsigned int columnIndex) {
    std::vector<std::vector<unsigned long>> result;
    result.push_back(row);

    const auto & columnValue = row[columnIndex];

    auto nextRow = reader->GetNextRow();
    if (nextRow.empty()) {
        return std::make_pair(result, nextRow);
    }

    while (!nextRow.empty() && nextRow[columnIndex] == columnValue) {
        result.push_back(nextRow);
        nextRow = reader->GetNextRow();
    }

    return std::make_pair(result, nextRow);
}

void SortMergeJoin::CartesianProduct(const std::vector<std::vector<unsigned long>> & a,
                                     const std::vector<std::vector<unsigned long>> & b) {
    for (const auto & rowA : a) {
        for (const auto & rowB : b) {
            std::vector<unsigned long> newRow;

            for (int i = 0; i < rowA.size(); i++) {
                if (i == columnAIndex) continue;
                newRow.push_back(rowA[i]);
            }

            newRow.push_back(rowB[columnBIndex]);

            for (int i = 0; i < rowB.size(); i++) {
                if (i == columnBIndex) continue;
                newRow.push_back(rowB[i]);
            }

            output->WriteNextRow(newRow);
        }
    }
}



