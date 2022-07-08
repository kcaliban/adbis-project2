//
// Created by fk on 08.07.22.
//

#include "JoinResult.h"
#include <sstream>
#include <iomanip>
#include <iostream>

JoinResult::JoinResult(const std::vector<std::string> &columnNames, unsigned int excludedColumnIndexOfA) {
    this->columnNames = columnNames;
    this->excludedColumnIndexOfA = excludedColumnIndexOfA;
}

void JoinResult::AddRow(const std::vector<std::string> * & rowA, const std::vector<std::string> * & rowB) {
    this->rows.emplace_back(
        std::pair(rowA, rowB)
    );
}

size_t JoinResult::GetRowCount() {
    return this->rows.size();
}

/*
std::string JoinResult::ToString(int n) {
    std::stringstream sstream;
    // HEADER
    // sstream << this->name << std::endl;
    sstream << "|";
    for (auto const & col : this->columnNames)
        sstream << " " << std::left << std::setw(25) << col << " |";
    sstream << std::endl << "-";
    for (int i = 0; i < this->columnNames.size() * (25 + 3); i++)
        sstream << "-";
    // ROWS
    for (int i = 0; i < std::min(this->rows.size(), (size_t) n); i++) {
        auto const & row = this->rows[i];
        sstream << std::endl << "|";
        for (auto const & col : row) {
            sstream << " " << std::left << std::setw(25) << col << " |";
        }
        sstream << std::endl << "-";
        for (int j = 0; j < this->columnNames.size() * (25 + 3); j++)
            sstream << "-";
    }
    return sstream.str();
}
*/