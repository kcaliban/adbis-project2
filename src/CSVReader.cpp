//
// Created by fk on 11.07.22.
//

#include <sstream>
#include <cstring>
#include "CSVReader.h"
#include <iostream>

CSVReader::CSVReader(std::istream * istream, const char & delim,
                     const std::string &tableName, bool binary) {
    this->istream = istream;
    this->delim = delim;
    this->binary = binary;
    if (!binary)
        this->columnNames = this->GetColumnNames();
    else {
        char chars[sizeof(unsigned int)];
        istream->read(chars, sizeof(unsigned int));
        memcpy(&this->columns, chars, sizeof(unsigned long));
    }
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

    if (!binary) {
        std::string nextRowString;
        if (!std::getline(*istream, nextRowString))
            return row;

        std::istringstream iss(nextRowString);
        std::string col;
        while (std::getline(iss, col, delim)) {
            row.push_back(std::stoull(col));
        }
    } else {
        // Last row reached
        if (istream->peek() == EOF)
            return row;
        for (unsigned int i = 0; i < columns; i++) {
            char chars[sizeof(unsigned long)];
            istream->read(chars, sizeof(unsigned long));

            unsigned long value;
            memcpy(&value, chars, sizeof(unsigned long));

            row.push_back(value);
        }
    }

    return row;
}

void CSVReader::JumpToBegin() {
    istream->clear();
    if (!binary) {
        // Skip column line
        istream->seekg(0);
        std::string tmp;
        std::getline(*istream, tmp);
    } else {
        // Skip column number identifier
        istream->seekg(sizeof(size_t));
    }
}

