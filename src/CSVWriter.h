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
                       const char & delim, unsigned long long cacheSize);
    void WriteNextRow(const std::vector<std::string> & row);
    virtual ~CSVWriter() {
        FlushCache();
        std::vector<std::vector<std::string>>().swap(cachedRows);
    }
    void FlushCache();
    void WriteRows(const std::vector<std::vector<std::string>> & rows);
    unsigned int getRowsWritten() const;
private:
    std::ostream * ostream;
    std::vector<std::vector<std::string>> cachedRows;
    char delim;
    unsigned long long cacheSize;
    unsigned long long rowsWritten;
    double GetCachedSize();
    void WriteRow(const std::vector<std::string> &);
};


#endif //PROJECT2_CSVWRITER_H
