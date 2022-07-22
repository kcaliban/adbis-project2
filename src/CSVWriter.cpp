//
// Created by fk on 11.07.22.
//

#include "CSVWriter.h"
#include <iostream>
#include <cmath>

CSVWriter::CSVWriter(std::ostream * ostream, const std::vector<std::string> &columnNames,
                     const char & delim, unsigned long long cacheSize) {
    this->ostream = ostream;
    std::string headerRow;
    for (const auto & column : columnNames) {
        if (!headerRow.empty())
            headerRow += delim;
        headerRow += column;
    }
    *ostream << headerRow << std::endl;

    this->delim = delim;
    this->cacheSize = cacheSize;
    this->rowsWritten = 0;

    size_t averageColumnSize = 30;
    size_t reservedSize = cacheSize /
                (sizeof(std::vector<std::string>) + sizeof(std::string) * columnNames.size() * averageColumnSize);
    this->cachedRows.reserve(round(reservedSize));
}

void CSVWriter::WriteNextRow(const std::vector<std::string> &row) {
    auto cachedSize = GetCachedSize();
    if (cachedSize > cacheSize) {
        FlushCache();
    }

    cachedRows.push_back(row);
}

void CSVWriter::WriteRows(const std::vector<std::vector<std::string>> & rows) {
    for (const auto & row : rows)
        WriteRow(row);
}

void CSVWriter::FlushCache() {
    for (const auto & row : cachedRows)
        WriteRow(row);
    cachedRows.clear();
}

double CSVWriter::GetCachedSize() {
    if (cachedRows.empty()) return 0;
    // This is just an approximation. String size varies per row and column entry.
    double size = sizeof(cachedRows) + cachedRows.size() * (
                    sizeof(cachedRows[0]) + cachedRows[0].size() * (
                        sizeof(cachedRows[0][0]) + cachedRows[0][0].length()
                )
            );

    return size;
}

void CSVWriter::WriteRow(const std::vector<std::string> &row) {
    std::string rowString;
    for (const auto & column : row) {
        if (!rowString.empty())
            rowString += delim;
        rowString += column;
    }
    *ostream << rowString << std::endl;
    rowsWritten++;
}

unsigned int CSVWriter::getRowsWritten() const {
    return rowsWritten;
}
