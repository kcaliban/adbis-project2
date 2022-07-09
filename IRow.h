//
// Created by fk on 09.07.22.
//

#ifndef PROJECT2_IROW_H
#define PROJECT2_IROW_H

#include <vector>
#include <string>


class IRow {
public:
    virtual ~IRow() {}
    virtual const std::vector<std::string> GetCols() const = 0;
    virtual const std::vector<const std::string*> GetColPtrs() const = 0;
};


#endif //PROJECT2_IROW_H
