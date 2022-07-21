//
// Created by fk on 21.07.22.
//

#ifndef PROJECT2_SORTMERGEJOIN_H
#define PROJECT2_SORTMERGEJOIN_H

#include "CSVReader.h"

class SortMergeJoin {
    explicit SortMergeJoin(CSVReader * A, const std::string& columnA,
                           CSVReader * B, const std::string& columnB,
                           std::ostream * ostream, unsigned int cacheSize);
    void Join();
    virtual ~SortMergeJoin() = default;
};


#endif //PROJECT2_SORTMERGEJOIN_H
