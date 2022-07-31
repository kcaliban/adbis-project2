//
// Created by fk on 11.07.22.
//

#ifndef PROJECT2_CSVREADER_H
#define PROJECT2_CSVREADER_H

#include <string>
#include <vector>
#include <fstream>

/*
 * CSVReader
 *
 * Works only for CSV files which do not have the delimiter in a column
 */
class CSVReader {
public:
    explicit CSVReader(std::istream * istream, const char & delim,
                       const std::string & tableName, bool binary = false);
    std::vector<unsigned long> GetNextRow();
    void JumpToBegin();
    std::vector<std::string> columnNames;
    std::string tableName;
    virtual ~CSVReader() = default;
private:
    std::istream * istream;
    bool binary;
    unsigned int columns;

    char delim;
    std::vector<std::string> GetColumnNames();
};


#endif //PROJECT2_CSVREADER_H
