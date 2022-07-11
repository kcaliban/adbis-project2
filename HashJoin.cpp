//
// Created by fk on 11.07.22.
//

#include "HashJoin.h"

void HashJoin::Join(CSVReader * A, const std::string& columnA,
                    CSVReader * B, const std::string& columnB,
                    const std::string & outputFile, unsigned int cacheSize)  {
    InitializeIndices(A, B, columnA, columnB);
    Build(A);
    Probe(A, B, columnB, outputFile, cacheSize);
}

void HashJoin::Reset() {
    this->columnBIndex = 0;
    this->columnAIndex = 0;
    std::unordered_map<std::string, std::vector<std::vector<std::string>>>().swap(this->hashMap);
}

void HashJoin::InitializeIndices(CSVReader *A, CSVReader *B,
                                 const std::string &columnA, const std::string &columnB) {
    auto const & columnNamesA = A->columnNames;
    for (int i = 0; i < columnNamesA.size(); i++)
        if (columnNamesA[i] == columnA)
            this->columnAIndex = i;

    auto const & columnNamesB = B->columnNames;
    for (int i = 0; i < columnNamesB.size(); i++)
        if (columnNamesB[i] == columnB)
            this->columnBIndex = i;
}

void HashJoin::Build(CSVReader *A) {
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

        row = A->GetNextRow();
    }

}

void HashJoin::Probe(CSVReader *A, CSVReader *B, const std::string &column,
                     const std::string &filename, unsigned int cacheSize) {
    std::cout << "PROBING" << std::endl;

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
        if (col != column) {
            columns.push_back(col);
        }
    }

    auto output = CSVWriter(filename, columns, ',', cacheSize);

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

            output.WriteNextRow( newRow);
        }

        row = B->GetNextRow();
    }
}
