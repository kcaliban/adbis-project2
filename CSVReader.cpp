//
// Created by fk on 11.07.22.
//

#include <sstream>
#include "CSVReader.h"

CSVReader::CSVReader(const std::string &filename, const char & delim, const std::string &tableName) {
    istream.open(filename);
    this->delim = delim;
    this->columnNames = this->GetNextRow();
    this->tableName = tableName;
}

std::vector<std::string> CSVReader::GetNextRow() {
    std::vector<std::string> row;

    std::string nextRowString;
    if (!std::getline(istream, nextRowString))
        return row;

    std::istringstream iss(nextRowString);
    std::string col;
    while (std::getline(iss, col, delim)) {
        row.push_back(col);
    }

    return row;
}

void CSVReader::JumpToBegin() {
    istream.clear();
    istream.seekg(0);
    this->GetNextRow();
}


