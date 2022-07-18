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
    explicit CSVWriter(const std::string & filename, const std::vector<std::string> & columnNames,
                       const char & delim, unsigned int cacheSize);
    void WriteNextRow(const std::vector<std::string> & row);
    virtual ~CSVWriter() {
        FlushCache();
        std::vector<std::vector<std::string>>().swap(cachedRows);
        ostream.close();
    }
private:
    std::ofstream ostream;
    std::vector<std::vector<std::string>> cachedRows;
    char delim;
    unsigned int cacheSize;
    void FlushCache();
    unsigned int GetCachedSize();
    void WriteRow(const std::vector<std::string> &);
};


#endif //PROJECT2_CSVWRITER_H
