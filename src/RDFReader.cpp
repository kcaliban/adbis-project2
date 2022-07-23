//
// Created by fk on 21.07.22.
//

#include "RDFReader.h"
#include "CSVWriter.h"
#include <unordered_map>
#include <iostream>
#include <algorithm>

std::string replaceString(std::string subject,
                          const std::string& search,
                          const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
    return subject;
}


RDFReader::RDFReader(const std::filesystem::path & file) {
    this->file = file;
}

std::vector<std::string> RDFReader::ToPartitionedCSV(const std::filesystem::path & outputDir) {
    std::filesystem::create_directories(outputDir);
    std::unordered_map<std::string, unsigned long> columnToInt;
    unsigned long long id = 0;

    std::vector<std::string> output;

    std::ifstream ifstream(file);
    std::unordered_map<std::string, CSVWriter*> tables;

    if (ifstream.is_open()) {
        std::string line;
        while (getline(ifstream, line)) {
            std::istringstream iss(line);
            std::string token;
            std::vector<std::string> tokens;
            while (getline(iss, token, '\t')) {
                tokens.push_back(token);
            }

            std::string subject = tokens[0], tableName = tokens[1], object = tokens[2];
            object = object.substr(0, object.length() - 2);

            // Get rid of special symbols in table name (used as the ouput file name) for watdiv.10M.nt
            tableName = replaceString(tableName, "<", "");
            tableName = replaceString(tableName, ">", "");
            tableName = replaceString(tableName, ".", "");
            tableName = replaceString(tableName, "~", "");
            tableName = replaceString(tableName, ":", "_");
            tableName = replaceString(tableName, "#", ":");
            tableName = replaceString(tableName, "/", "_");

            auto it = tables.find(tableName);
            if (it == tables.end()) {
                auto outputFile = std::filesystem::path(outputDir) / std::filesystem::path(tableName + ".csv");
                auto * ofstream = new std::ofstream(outputFile);
                auto result = tables.insert(
                        {tableName, new CSVWriter( ofstream, {"Subject", "Object"}, ',')}
                );
                it = result.first;
            }

            auto itSub = columnToInt.find(subject);
            if (itSub == columnToInt.end()) {
                itSub = columnToInt.insert({subject, id}).first;
                id++;
            }

            auto itObj = columnToInt.find(object);
            if (itObj == columnToInt.end()) {
                itObj = columnToInt.insert({object, id}).first;
                id++;
            }

            it->second->WriteNextRow({itSub->second, itObj->second});
        }
    }
    ifstream.close();

    std::cout << "Created files:" << std::endl;
    std::cout << "\t";
    std::ostringstream oss;
    for (auto & it : tables) {
        delete it.second;
        auto outputFile = std::filesystem::path(outputDir) / std::filesystem::path(it.first + ".csv");
        output.push_back(outputFile);
        oss << outputFile << ", ";
    }
    std::cout << oss.str().substr(0, std::min((int) oss.str().length(), 100)) << "..." << std::endl;

    auto outputFile = std::filesystem::path(outputDir) / std::filesystem::path("legend.csv");
    std::ofstream ofstream(outputFile);
    for (auto const & [key,val] : columnToInt)
        ofstream << std::to_string(val) << "," << key << std::endl;
    ofstream.close();
    std::cout << "Legend (number,object): " << outputFile << std::endl;

    return output;
}
