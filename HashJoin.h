//
// Created by fk on 08.07.22.
//

#ifndef PROJECT2_HASHJOIN_H
#define PROJECT2_HASHJOIN_H

#include "Relation.h"
#include <unordered_map>
#include <vector>
#include <string>

class HashJoin {
public:
    explicit HashJoin() = default;
    virtual ~HashJoin() = default;
    Relation Join(const Relation&, const std::string&, const Relation&, const std::string&);
private:
    std::vector<std::string> hashMapColumnNames;
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> hashMap;
    void Build(const Relation&, const std::string&);
    Relation Probe(const Relation&, const Relation&, const std::string&);
};


#endif //PROJECT2_HASHJOIN_H
