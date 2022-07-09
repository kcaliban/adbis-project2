//
// Created by fk on 09.07.22.
//

#include "PtrRow.h"
#include <iostream>

PtrRow::PtrRow(std::vector<const std::string *> contents) {
    this->contents = contents;
}

const std::vector<std::string> PtrRow::GetCols() const {
    auto result = std::vector<std::string>{};
    for (auto const & ptr : contents) {
        auto const & str = *ptr;
        result.push_back(str);
    }
    return result;
}

const std::vector<const std::string *> PtrRow::GetColPtrs() const {
    return this->contents;
}
