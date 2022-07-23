//
// Created by fk on 11.07.22.
//

#include "HashJoin.h"
#include <cmath>

HashJoin::HashJoin(CSVReader *A, const std::string &columnA, CSVReader *B, const std::string &columnB,
                   std::ostream *ostream, unsigned long long hashTableSize) {
    this->A = A;
    this->columnA = columnA;
    this->B = B;
    this->columnB = columnB;
    this->ostream = ostream;
    this->hashTableSize = hashTableSize;
    this->totalRowsInHashMap = 0;

    InitializeIndices();
    InitializeOutput();
}

unsigned long long HashJoin::GetHashTableSize() {
    // Approximate
    if (hashMap.empty()) return 0;

    auto size = hashMap.size() * (sizeof(std::vector<std::vector<unsigned long>>*) + sizeof(unsigned long))  // Size of map
              + hashMap.size() * sizeof(std::vector<std::vector<unsigned long>>) // Size vectors pointed to
              // Size of rows, x1.5 for allocation overhead
              + ((unsigned long long)round(1.5 * totalRowsInHashMap)) * (sizeof(unsigned long) * A->columnNames.size() + sizeof(std::vector<unsigned long>));
    return size;
}

void HashJoin::Join()  {
    std::cout << "\tBUILDING" << std::endl;

    auto row = A->GetNextRow();
    while (!row.empty()) {
        auto const &col = row[columnAIndex];
        // Initialize vector in hashMap if required; find should be constant time
        auto it = hashMap.find(col);
        if (it == hashMap.end()) {
            auto vector = new std::vector<std::vector<unsigned long>>();
            auto result = hashMap.insert({col, vector});
            it = result.first;
        }
        it->second->push_back(row);
        totalRowsInHashMap++;

        if (GetHashTableSize() > hashTableSize) {
            for (auto itr : hashMap) {
                itr.second->shrink_to_fit();
            }
            Probe();
            for (auto itr : hashMap) {
                delete itr.second;
            }
            hashMap.clear();
            totalRowsInHashMap = 0;
            std::cout << "\tBUILDING" << std::endl;
        }

        row = A->GetNextRow();
    }

    for (auto itr : hashMap) {
        itr.second->shrink_to_fit();
    }
    Probe();
    for (auto itr : hashMap) {
        delete itr.second;
    }
    hashMap.clear();
    std::cout << "\tROWS WRITTEN: " << output->getRowsWritten() << std::endl;
}

void HashJoin::InitializeOutput() {
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

void HashJoin::InitializeIndices() {
    auto const & columnNamesA = A->columnNames;
    for (int i = 0; i < columnNamesA.size(); i++)
        if (columnNamesA[i] == columnA)
            this->columnAIndex = i;

    auto const & columnNamesB = B->columnNames;
    for (int i = 0; i < columnNamesB.size(); i++)
        if (columnNamesB[i] == columnB)
            this->columnBIndex = i;
}

void HashJoin::Probe() {
    std::cout << "\tPROBING" << std::endl;

    B->JumpToBegin();
    auto row = B->GetNextRow();

    while (!row.empty()) {
        auto const & joinCol = row[columnBIndex];
        auto const & it = hashMap.find(joinCol);
        if (it == hashMap.end()) { row = B->GetNextRow(); continue; }

        auto const & mapRows = it->second;
        for (auto const & mapRow : *mapRows) {
            std::vector<unsigned long> newRow;

            for (int i = 0; i < mapRow.size(); i++) {
                if (i == columnAIndex) continue;
                newRow.push_back(mapRow[i]);
            }

            newRow.push_back(row[columnBIndex]);

            for (int i = 0; i < row.size(); i++) {
                if (i == columnBIndex) continue;
                newRow.push_back(row[i]);
            }

            output->WriteNextRow(newRow);
        }

        row = B->GetNextRow();
    }
}
