//
// Created by fk on 07.07.22.
//

#include "TwoColumnTable.h"

TwoColumnTable::TwoColumnTable(const std::string& name) {
    this->name = name;
}

TwoColumnTable::~TwoColumnTable() = default;

void TwoColumnTable::AddRow(const std::pair<std::string, std::string>& pair) {
    this->rows.push_back( pair );
}

std::vector<std::pair<std::string, std::string>> TwoColumnTable::GetRows() {
    return this->rows;
}

std::string TwoColumnTable::GetName() {
    return this->name;
}
