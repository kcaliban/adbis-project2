//
// Created by fk on 11.07.22.
//

#include "CSVWriter.h"
#include <iostream>

CSVWriter::CSVWriter(const std::string &filename, const std::vector<std::string> &columnNames,
                     const char & delim, unsigned int cacheSize) {
    ostream.open(filename);
    std::string headerRow;
    for (const auto & column : columnNames) {
        if (!headerRow.empty())
            headerRow += delim;
        headerRow += column;
    }
    ostream << headerRow << std::endl;

    this->delim = delim;
    this->cacheSize = cacheSize;
}

void CSVWriter::WriteNextRow(const std::vector<std::string> &row) {
    if (GetCachedSize() > cacheSize) {
        FlushCache();
    }

    cachedRows.push_back(row);
}

void CSVWriter::FlushCache() {
    for (const auto & row : cachedRows)
        WriteRow(row);
    cachedRows.clear();
}

unsigned int CSVWriter::GetCachedSize() {
    if (cachedRows.empty()) return 0;
    // This is just an approximation. String size varies per row and column entry.
    size_t size = sizeof(cachedRows) + cachedRows.size() * (
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
    ostream << rowString << std::endl;
}