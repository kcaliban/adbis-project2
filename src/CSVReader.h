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
    explicit CSVReader(std::istream * istream, const char & delim, const std::string & tableName);
    std::vector<std::string> GetNextRow();
    void JumpToBegin();
    std::vector<std::string> columnNames;
    std::string tableName;
    virtual ~CSVReader() = default;
    std::vector<std::vector<std::string>> GetAllRows();
private:
    std::istream * istream;

    char delim;
};


#endif //PROJECT2_CSVREADER_H
