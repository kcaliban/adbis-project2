//
// Created by fk on 11.07.22.
//

#ifndef PROJECT2_CSVWRITER_H
#define PROJECT2_CSVWRITER_H

#include <string>
#include <vector>
#include <fstream>

class CSVWriter {
public:
    explicit CSVWriter(std::ostream * ostream, const std::vector<std::string> & columnNames,
                       const char & delim, bool binary = false);
    void WriteNextRow(const std::vector<unsigned long> & row);
    virtual ~CSVWriter() = default;
    unsigned long long getRowsWritten() const;
private:
    std::ostream * ostream;
    std::vector<std::vector<unsigned long long>> cachedRows;
    char delim;
    bool binary;
    unsigned long long rowsWritten;
};


#endif //PROJECT2_CSVWRITER_H
