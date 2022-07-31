//
// Created by fk on 11.07.22.
//

#include "CSVWriter.h"
#include <iostream>
#include <cmath>
#include <cstring>

CSVWriter::CSVWriter(std::ostream * ostream, const std::vector<std::string> &columnNames,
                     const char & delim, bool binary) {
    this->ostream = ostream;
    this->binary = binary;
    std::string headerRow;
    if (!binary) {
        for (const auto & column : columnNames) {
            if (!headerRow.empty())
                headerRow += delim;
            headerRow += column;
        }
        *ostream << headerRow << std::endl;
    } else {
        char chars[sizeof(unsigned int)];
        auto columns = (unsigned int) columnNames.size();
        memcpy(chars, &columns, sizeof(unsigned int));
        ostream->write(chars, sizeof(columns));
    }

    this->delim = delim;
    this->rowsWritten = 0;
}

void CSVWriter::WriteNextRow(const std::vector<unsigned long> &row) {
    if (!binary) {
        std::string rowString;
        for (const auto & column : row) {
            if (!rowString.empty())
                rowString += delim;
            rowString += std::to_string(column);
        }
        *ostream << rowString << std::endl;
        rowsWritten++;
    } else {
        for (const auto & column : row) {
            char chars[sizeof(column)];
            memcpy(chars, &column, sizeof(column));
            ostream->write(chars, sizeof(column));
        }
        rowsWritten++;
    }
}

unsigned long long CSVWriter::getRowsWritten() const {
    return rowsWritten;
}
