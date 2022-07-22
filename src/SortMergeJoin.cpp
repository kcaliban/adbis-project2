//
// Created by fk on 21.07.22.
//

#include "SortMergeJoin.h"
#include "ExternalSort.h"
#include <unordered_set>
#include <iostream>

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

    this->output = new CSVWriter(ostream, columns, ',', cacheSize);
}

void SortMergeJoin::Sort() {
    std::string sortedAFileName = A->tableName + "_sorted";
    std::filesystem::path sortedAOutputPath = tempDir / sortedAFileName;
    ExternalSort externalSortA(A, columnA, cacheSize, tempDir, sortedAOutputPath);
    externalSortA.Sort();
    istreamA = new std::ifstream(sortedAOutputPath);
    sortedA = new CSVReader(istreamA, ',', sortedAFileName);

    std::string sortedBFileName = B->tableName + "_sorted";
    std::filesystem::path sortedBOutputPath = tempDir / sortedBFileName;
    ExternalSort externalSortB(B, columnB, cacheSize, tempDir, sortedBOutputPath);
    externalSortB.Sort();
    istreamB = new std::ifstream(sortedBOutputPath);
    sortedB = new CSVReader(istreamB, ',', sortedBFileName);
}

void SortMergeJoin::Join() {
    std::cout << "\tSORT" << std::endl;
    Sort();
    std::cout << "\tMERGE" << std::endl;
    Merge();
    output->FlushCache();
    std::cout << "\tROWS WRITTEN: " << output->getRowsWritten() << std::endl;
}

void SortMergeJoin::Merge() {
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
}

std::pair<std::vector<std::vector<std::string>>,std::vector<std::string>>
    SortMergeJoin::Collect(CSVReader * reader,
                           const std::vector<std::string> & row,
                           unsigned int columnIndex) {
    std::vector<std::vector<std::string>> result;
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

void SortMergeJoin::CartesianProduct(const std::vector<std::vector<std::string>> & a,
                                     const std::vector<std::vector<std::string>> & b) {
    for (const auto & rowA : a) {
        for (const auto & rowB : b) {
            std::vector<std::string> newRow;

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



