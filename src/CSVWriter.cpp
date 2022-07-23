//
// Created by fk on 11.07.22.
//

#include "CSVWriter.h"
#include <iostream>
#include <cmath>

CSVWriter::CSVWriter(std::ostream * ostream, const std::vector<std::string> &columnNames,
                     const char & delim) {
    this->ostream = ostream;
    std::string headerRow;
    for (const auto & column : columnNames) {
        if (!headerRow.empty())
            headerRow += delim;
        headerRow += column;
    }
    *ostream << headerRow << std::endl;

    this->delim = delim;
    this->rowsWritten = 0;
}

void CSVWriter::WriteNextRow(const std::vector<unsigned long> &row) {
    std::string rowString;
    for (const auto & column : row) {
        if (!rowString.empty())
            rowString += delim;
        rowString += std::to_string(column);
    }
    *ostream << rowString << std::endl;
    rowsWritten++;
}

unsigned long long CSVWriter::getRowsWritten() const {
    return rowsWritten;
}
