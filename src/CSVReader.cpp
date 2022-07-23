//
// Created by fk on 11.07.22.
//

#include <sstream>
#include "CSVReader.h"

CSVReader::CSVReader(std::istream * istream, const char & delim, const std::string &tableName) {
    this->istream = istream;
    this->delim = delim;
    this->columnNames = this->GetColumnNames();
    this->tableName = tableName;
}

std::vector<std::string> CSVReader::GetColumnNames() {
    std::vector<std::string> row;

    std::string nextRowString;
    if (!std::getline(*istream, nextRowString))
        return row;

    std::istringstream iss(nextRowString);
    std::string col;
    while (std::getline(iss, col, delim)) {
        row.push_back(col);
    }

    return row;
}

std::vector<unsigned long> CSVReader::GetNextRow() {
    std::vector<unsigned long> row;

    std::string nextRowString;
    if (!std::getline(*istream, nextRowString))
        return row;

    std::istringstream iss(nextRowString);
    std::string col;
    while (std::getline(iss, col, delim)) {
        row.push_back(std::stoull(col));
    }

    return row;
}

void CSVReader::JumpToBegin() {
    istream->clear();
    istream->seekg(0);
    // Skip column line
    std::string tmp;
    std::getline(*istream, tmp);
}

unsigned int CSVReader::GetNumberOfRows() {
    unsigned int result = 0;
    std::string row;
    while (std::getline(*istream, row))
        result++;
    JumpToBegin();
    return result;
}


