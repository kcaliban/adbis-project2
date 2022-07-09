//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_HASHJOIN_H
#define PROJECT2_HASHJOIN_H

#include "Relation.h"
#include <unordered_map>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <array>

template <int COLS_A, int COLS_B>
class HashJoin {
public:
    explicit HashJoin() = default;
    Relation<COLS_A + COLS_B - 1> * Join(const Relation<COLS_A> * A, const std::string& columnA,
                                         const Relation<COLS_B> * B , const std::string& columnB,
                                         bool dry = false) {
        InitializeIndices(A, B, columnA, columnB);
        Build(A);
        auto result = Probe(A, B, columnB, dry);
        return result;
    };
private:
    unsigned int columnAIndex;
    unsigned int columnBIndex;
    std::unordered_map<std::string, std::vector<std::array<std::string*, COLS_A>>> hashMap;

    void InitializeIndices(const Relation<COLS_A> * A, const Relation<COLS_B> * B,
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

    void Build(const Relation<COLS_A> * A) {
        std::cout << "BUILDING" << std::endl;

        auto const &rows = A->GetRows();
        for (auto const &row: rows) {
            auto const &col = *(row[columnAIndex]);
            // Initialize vector in hashMap if required; find should be constant time
            auto it = hashMap.find(col);
            if (it == hashMap.end()) {
                auto result = hashMap.insert({col, {}});
                it = result.first;
            }
            it->second.push_back(row);
        }
    };

    Relation<COLS_A + COLS_B - 1> * Probe(const Relation<COLS_A> * A, const Relation<COLS_B> * B, const std::string& column, bool dry) {
        std::cout << "PROBING" << std::endl;

        // Initialize output
        auto const & columnNamesA = A->GetColumnNames();
        auto const & columnNamesB = B->GetColumnNames();
        auto relationName = A->GetName() + "_" + B->GetName();
        std::array<std::string, COLS_A + COLS_B - 1> columns;

        // Left over column names from A, Name from A + Name from B, Left over column names from B
        int j = 0;
        for (int i = 0; i < columnNamesA.size(); i++) {
            if (i != columnAIndex) {
                columns[j] = columnNamesA[i];
                j++;
            }
        }
        columns[j++] = A->GetName() + "_" + B->GetName();
        for (auto const & col : columnNamesB) {
            if (col != column) {
                columns[j] = col;
                j++;
            }
        }

        auto output = new Relation<COLS_A + COLS_B - 1>(relationName, columns,
                                                        std::max(A->GetRows().size(),
                                                                 B->GetRows().size()));

        int idx;
        for (int i = 0; i < columnNamesB.size(); i++)
            if (columnNamesB[i] == column)
                idx = i;

        int i = 0;
        auto const & rows = B->GetRows();
        for (auto const & row : rows) {
            auto const & joinCol = *(row[idx]);
            auto const & it = hashMap.find(joinCol);
            if (it == hashMap.end()) continue;

            auto const & mapRows = it->second;
            for (auto const & mapRow : mapRows) {
                i++;
                if (dry) continue;

                std::array<std::string*, COLS_A + COLS_B - 1> newRow;

                int j = 0;
                for (int i = 0; i < mapRow.size(); i++) {
                    if (i == columnAIndex) continue;
                    newRow[j] = mapRow[i];
                    j++;
                }

                newRow[j++] = row[columnBIndex];

                for (int i = 0; i < row.size(); i++) {
                    if (i == columnBIndex) continue;
                    newRow[j] = row[i];
                    j++;
                }

                output->AddRow(newRow);
            }
        }
        std::cout << "Rows: " << i << std::endl;

        return output;
    }
};

#endif //PROJECT2_HASHJOIN_H
