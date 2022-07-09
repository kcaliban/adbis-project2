//
// Created by fk on 08.07.22.
//

#include "HashJoin.h"
#include "PtrRow.h"
#include <iostream>
#include <algorithm>
#include <math.h>

Relation *
HashJoin::Join(const Relation * A, const std::string & columnA,
               const Relation * B, const std::string & columnB) {
    // Determine the smaller of the two relations, handle accordingly
    if (A->GetRows().size() < B->GetRows().size()) {
        InitializeIndices(A, B, columnA, columnB);
        Build(A);
        auto result = Probe( A, B, columnB);
        return result;
    } else {
        InitializeIndices(B, A, columnB, columnA);
        Build(B);
        auto result = Probe(B, A, columnA);
        return result;
    }
}


void HashJoin::Build(const Relation * A) {
    std::cout << "BUILDING" << std::endl;

    auto const & rows = A->GetRows();
    for (auto const & row : rows) {
        auto const & cols = row->GetColPtrs();
        auto const & col = *(cols[columnAIndex]);
        // Initialize vector in hashMap if required; find should be constant time
        auto it = hashMap.find(col);
        if (it == hashMap.end()) {
            auto result = hashMap.insert({col, {}});
            it = result.first;
        }
        it->second.push_back(row);
    }
}

Relation * HashJoin::Probe(const Relation * A, const Relation * B, const std::string& column) {
    std::cout << "PROBING" << std::endl;

    // Initialize output
    auto const & columnNamesA = A->GetColumnNames();
    auto const & columnNamesB = B->GetColumnNames();
    auto relationName = A->GetName() + "_" + B->GetName();
    std::vector<std::string> columns;

    // Left over column names from A, Name from A + Name from B, Left over column names from B
    for (int i = 0; i < columnNamesA.size(); i++) {
        if (i != columnAIndex)
            columns.push_back(columnNamesA[i]);
    }
    columns.push_back(A->GetName() + "_" + B->GetName());
    for (auto const & col : columnNamesB) {
        if (col != column)
            columns.push_back(col);
    }

    auto output = new Relation(relationName, columns);

    int idx;
    for (int i = 0; i < columnNamesB.size(); i++)
        if (columnNamesB[i] == column)
            idx = i;

    auto const & rows = B->GetRows();
    for (auto const & row : rows) {
        auto const & colBPtrs = row->GetColPtrs();
        auto const & joinCol = *(colBPtrs[idx]);
        auto const & it = hashMap.find(joinCol);
        if (it == hashMap.end()) continue;

        auto const & mapRows = it->second;

        for (auto const & mapRow : mapRows) {
            std::vector<const std::string*> ptrs;
            auto const & colAPtrs = mapRow->GetColPtrs();

            for (int i = 0; i < colAPtrs.size(); i++) {
                if (i == columnAIndex) continue;
                ptrs.push_back(colAPtrs[i]);
            }

            ptrs.push_back(colBPtrs[columnBIndex]);

            for (int i = 0; i < colBPtrs.size(); i++) {
                if (i == columnBIndex) continue;
                ptrs.push_back(colBPtrs[i]);
            }

            output->AddRow(new PtrRow(ptrs));
        }
    }

    return output;
}


void HashJoin::InitializeIndices(const Relation * A, const Relation * B,
                                 const std::string & columnA, const std::string & columnB) {
    auto const & columnNamesA = A->GetColumnNames();
    for (int i = 0; i < columnNamesA.size(); i++)
        if (columnNamesA[i] == columnA)
            this->columnAIndex = i;

    auto const & columnNamesB = B->GetColumnNames();
    for (int i = 0; i < columnNamesB.size(); i++)
        if (columnNamesB[i] == columnB)
            this->columnBIndex = i;
}
