//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_JOINRESULT_H
#define PROJECT2_JOINRESULT_H

#include <vector>
#include <string>

class JoinResult {
public:
    JoinResult(const std::vector<std::string> & columnNames, unsigned int excludedColumnIndexOfA, size_t reservedSpace);
    virtual ~JoinResult() = default;
    void AddRow(const std::vector<std::string>*, const std::vector<std::string>*);
    size_t GetRowCount();
   //  std::string ToString(int n);
private:
    unsigned int excludedColumnIndexOfA;
    std::vector<std::string> columnNames;
    // A row points to the two rows of the other table
    std::vector<std::pair<const std::vector<std::string>*,
                          const std::vector<std::string>*>> rows;

};


#endif //PROJECT2_JOINRESULT_H
