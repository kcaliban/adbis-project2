//
// Created by fk on 07.07.22.
//

#include "Relation.h"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>

Relation::Relation(const std::string & name, const std::vector<std::string> & columnNames) {
    this->name = name;
    this->columnNames = columnNames;
}

Relation::~Relation() = default;

void Relation::AddRow(IRow *row) {
    this->rows.push_back(row);
}

const std::list<IRow*> & Relation::GetRows() const {
    return this->rows;
}

std::string Relation::GetName() const {
    return this->name;
}

const std::vector<std::string> & Relation::GetColumnNames() const {
    return this->columnNames;
}

std::string Relation::ToString(int n) {
    std::stringstream sstream;
    // HEADER
    sstream << this->name << std::endl;
    sstream << "|";
    for (auto const & col : this->columnNames)
        sstream << " " << std::left << std::setw(25) << col << " |";
    sstream << std::endl << "-";
    for (int i = 0; i < this->columnNames.size() * (25 + 3); i++)
        sstream << "-";
    // ROWS
    auto rowIterator = this->rows.begin();
    for (int i = 0; i < std::min(this->rows.size(), (size_t) n); i++) {
        sstream << std::endl << "|";
        auto const & cols = (*rowIterator)->GetCols();
        for (auto const & col : cols) {
            sstream << " " << std::left << std::setw(25) << col << " |";
        }
        sstream << std::endl << "-";
        for (int j = 0; j < this->columnNames.size() * (25 + 3); j++)
            sstream << "-";

        rowIterator++;
    }
    return sstream.str();
}

Relation Relation::SelectWhere(const std::string & columnName, const std::string & columnEntry) const {
    Relation output = Relation(this->name, this->columnNames);

    int idx;
    for (int i = 0; i < this->columnNames.size(); i++)
        if (this->columnNames[i] == columnName)
            idx = i;

    for (auto const & row : this->rows) {
        auto const & cols = row->GetCols();

        if (cols[idx] == columnEntry) {
            output.AddRow(row);
        }
    }

    return output;
}
