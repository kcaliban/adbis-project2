//
// Created by fk on 09.07.22.
//

#include "Row.h"

Row::Row(std::vector<std::string> contents) {
    this->contents = contents;
}

const std::vector<std::string> Row::GetCols() const {
    return this->contents;
}

const std::vector<const std::string *> Row::GetColPtrs() const {
    std::vector<const std::string*> ptrs;
    for (int i = 0; i < contents.size(); i++) {
        ptrs.push_back(&(contents[i]));
    }
    return ptrs;
}

