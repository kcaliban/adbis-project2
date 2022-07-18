//
// Created by fk on 11.07.22.
//

#include "HashJoin.h"

HashJoin::HashJoin(CSVReader *A, const std::string &columnA, CSVReader *B, const std::string &columnB,
                   std::ostream *ostream, unsigned int cacheSize) {
    this->A = A;
    this->columnA = columnA;
    this->B = B;
    this->columnB = columnB;
    this->ostream = ostream;
    this->cacheSize = cacheSize;

    InitializeIndices();
    InitializeOutput();
}

unsigned int HashJoin::GetHashTableSize() {
    // Approximate
    if (hashMap.empty()) return 0;

    auto rows = hashMap.begin()->second;
    size_t size = hashMap.size() * (sizeof(rows) + rows.size() * (
            sizeof(rows[0]) + rows[0].size() * (
                    sizeof(rows[0][0]) + rows[0][0].length()
            )
    ));

    return size;
}

void HashJoin::Join(unsigned int hashTableSize)  {
    std::cout << "BUILDING" << std::endl;

    auto row = A->GetNextRow();

    while (!row.empty()) {
        auto const &col = row[columnAIndex];
        // Initialize vector in hashMap if required; find should be constant time
        auto it = hashMap.find(col);
        if (it == hashMap.end()) {
            auto result = hashMap.insert({col, {}});
            it = result.first;
        }
        it->second.push_back(row);

        if (GetHashTableSize() > hashTableSize) {
            Probe();
            hashMap.clear();
            std::cout << "BUILDING" << std::endl;
        }

        row = A->GetNextRow();
    }

    Probe();
    output->FlushCache();
}

void HashJoin::InitializeOutput() {
    // Initialize output
    auto const & columnNamesA = A->columnNames;
    auto const & columnNamesB = B->columnNames;
    std::vector<std::string> columns;

    // Left over column names from A, Name from A + Name from B, Left over column names from B
    for (int i = 0; i < columnNamesA.size(); i++) {
        if (i != columnAIndex) {
            columns.push_back(columnNamesA[i]);
        }
    }
    columns.push_back(A->tableName + "_" + B->tableName);
    for (auto const & col : columnNamesB) {
        if (col != columnB) {
            columns.push_back(col);
        }
    }

    this->output = new CSVWriter(ostream, columns, ',', cacheSize);
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
    std::cout << "PROBING" << std::endl;

    B->JumpToBegin();
    auto row = B->GetNextRow();

    while (!row.empty()) {
        auto const & joinCol = row[columnBIndex];
        auto const & it = hashMap.find(joinCol);
        if (it == hashMap.end()) { row = B->GetNextRow(); continue; }

        auto const & mapRows = it->second;
        for (auto const & mapRow : mapRows) {
            std::vector<std::string> newRow;

            for (int i = 0; i < mapRow.size(); i++) {
                if (i == columnAIndex) continue;
                newRow.push_back(mapRow[i]);
            }

            newRow.push_back(row[columnBIndex]);

            for (int i = 0; i < row.size(); i++) {
                if (i == columnBIndex) continue;
                newRow.push_back(row[i]);
            }

            output->WriteNextRow( newRow);
        }

        row = B->GetNextRow();
    }
}
