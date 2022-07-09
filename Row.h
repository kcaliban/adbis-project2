//
// Created by fk on 09.07.22.
//

#ifndef PROJECT2_ROW_H
#define PROJECT2_ROW_H

#include "IRow.h"

class Row : public IRow {
public:
    explicit Row(std::vector<std::string>);
    const std::vector<std::string> GetCols() const override;
    const std::vector<const std::string*> GetColPtrs() const override;
private:
    std::vector<std::string> contents;
};


#endif //PROJECT2_ROW_H
