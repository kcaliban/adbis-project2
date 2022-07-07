//
// Created by fk on 07.07.22.
//

#ifndef PROJECT2_TWOCOLUMNTABLE_H
#define PROJECT2_TWOCOLUMNTABLE_H


#include <string>
#include <vector>

class TwoColumnTable {
public:
    explicit TwoColumnTable(const std::string&);
    virtual ~TwoColumnTable();
    void AddRow(const std::pair<std::string,std::string>&);
    std::vector<std::pair<std::string,std::string>> GetRows();
    std::string GetName();
private:
    std::vector<std::pair<std::string,std::string>> rows;
    std::string name;
};


#endif //PROJECT2_TWOCOLUMNTABLE_H
