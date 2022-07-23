//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_RDFREADER_H
#define PROJECT2_RDFREADER_H

#include <string>
#include <vector>
#include <filesystem>

class RDFReader {
public:
    explicit RDFReader(const std::filesystem::path & file);
    std::vector<std::string> ToPartitionedCSV(const std::filesystem::path & outputDir);
private:
    std::filesystem::path file;
};


#endif //PROJECT2_RDFREADER_H
