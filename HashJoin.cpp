//
// Created by fk on 08.07.22.
//

#include "HashJoin.h"
#include <iostream>

Relation
HashJoin::Join(const Relation & A, const std::string & columnA,
               const Relation & B, const std::string & columnB) {
    // Determine the smaller of the two relations, handle accordingly
    if (A.GetRows().size() < B.GetRows().size()) {
        Build(A, columnA);
        Relation result = Probe( A, B, columnB);
        return result;
    } else {
        Build(B, columnB);
        Relation result = Probe(B, A, columnA);
        return result;
    }
}


void HashJoin::Build(const Relation & A, const std::string & column) {
    std::cout << "BUILDING" << std::endl;

    int idx;
    auto const & columnNames = A.GetColumnNames();
    for (int i = 0; i < columnNames.size(); i++)
        if (columnNames[i] == column)
            idx = i;
        else
            this->hashMapColumnNames.push_back(columnNames[i]);

    auto const & rows = A.GetRows();
    for (auto const & row : rows) {
        auto const & col = row[idx];
        // Initialize vector in hashMap if required; find should be constant time
        auto it = hashMap.find(col);
        if (it == hashMap.end()) {
            auto result = hashMap.insert({col, {}});
            it = result.first;
        }
        // Create new row
        std::vector<std::string> newRow;
        // Add other columns of current row to new row
        for (int i = 0; i < columnNames.size(); i++) {
            if (i == idx)
                continue;
            newRow.push_back(row[i]);
        }
        it->second.push_back(newRow);
    }
}

Relation HashJoin::Probe(const Relation & A, const Relation & B, const std::string& column) {
    std::cout << "PROBING" << std::endl;
    // Initialize output relation
    auto const & columnNamesB = B.GetColumnNames();
    auto relationName = A.GetName() + "_" + B.GetName();
    // Left over column names from A, Name from A + Name from B, Left over column names from B
    std::vector<std::string> columns = this->hashMapColumnNames;
    columns.push_back(A.GetName() + "_" + B.GetName());
    for (auto const & col : columnNamesB) {
        if (col != column)
            columns.push_back(col);
    }
    auto output = Relation(relationName, columns);

    int idx;
    for (int i = 0; i < columnNamesB.size(); i++)
        if (columnNamesB[i] == column)
            idx = i;

    auto const & rows = B.GetRows();
    for (auto const & row : rows) {
        auto const & joinCol = row[idx];
        auto const & it = hashMap.find(joinCol);
        if (it == hashMap.end()) continue;

        for (auto const & mapRow : it->second) {
            std::vector<std::string> newRow = {};
            for (auto const & mapRowColumn : mapRow) {
                newRow.push_back(mapRowColumn);
            }
            for (const auto & col : row) {
                newRow.push_back(col);
            }
            output.AddRow(newRow);
        }
    }

    return output;
}
