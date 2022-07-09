//
// Created by fk on 09.07.22.
//

#ifndef PROJECT2_PTRROW_H
#define PROJECT2_PTRROW_H

#include "IRow.h"

// Watch out for pointers to vector elements, if that vector is reallocated.
class PtrRow : public IRow {
public:
    explicit PtrRow(std::vector<const std::string*>);
    const std::vector<std::string> GetCols() const override;
    const std::vector<const std::string*> GetColPtrs() const override;
private:
    std::vector<const std::string*> contents;
};


#endif //PROJECT2_PTRROW_H
