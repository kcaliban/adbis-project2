#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <sstream>
#include "TwoColumnTable.h"

std::unordered_map<std::string, TwoColumnTable> getPartitionTablesFromRDF(const std::string& fileName) {
    // Idea: Multi-threaded reading of file
    std::ifstream ifstream(fileName);
    std::unordered_map<std::string, TwoColumnTable> tables;

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

            auto it = tables.find(tableName);
            if (it == tables.end()) {
                auto result = tables.insert({tableName, TwoColumnTable(tableName)});
                it = result.first;
            }
            it->second.AddRow({subject, object});
        }
    }
    ifstream.close();

    return tables;
}

int main() {
    auto tables = getPartitionTablesFromRDF("./watdiv.10M.nt");
    /*
    for (auto kv : tables) {
        std::cout << kv.first << ":\t" << kv.second.GetRows().size() << std::endl;
    }
    */

    return 0;
}
