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
    explicit CSVReader(const std::string & filename, const char & delim, const std::string & tableName);
    std::vector<std::string> GetNextRow();
    std::vector<std::string> columnNames;
    std::string tableName;
    virtual ~CSVReader() { istream.close(); }
private:
    std::ifstream istream;

    char delim;
};


#endif //PROJECT2_CSVREADER_H
