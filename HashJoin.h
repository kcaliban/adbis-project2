//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_HASHJOIN_H
#define PROJECT2_HASHJOIN_H

#include "Relation.h"
#include "JoinResult.h"
#include <unordered_map>
#include <vector>
#include <string>

class HashJoin {
public:
    explicit HashJoin() = default;
    virtual ~HashJoin() = default;
    JoinResult * Join(const Relation *, const std::string&, const Relation *, const std::string&);
private:
    unsigned int columnAIndex;
    unsigned int columnBIndex;
    std::unordered_map<std::string, std::vector<const std::vector<std::string> *>> hashMap;

    void InitializeIndices(const Relation *, const Relation *,
                           const std::string &, const std::string &);
    void Build(const Relation *);
    JoinResult * Probe(const Relation *, const Relation *, const std::string&);
};


#endif //PROJECT2_HASHJOIN_H
