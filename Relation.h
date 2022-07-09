//
// Created by fk on 07.07.22.
//

#ifndef PROJECT2_RELATION_H
#define PROJECT2_RELATION_H


#include <string>
#include <list>
#include "IRow.h"

// Improvement: Generic class with type list for columns?
class Relation {
public:
    explicit Relation(const std::string&, const std::vector<std::string>&);
    virtual ~Relation();
    void AddRow(IRow *);
    const std::list<IRow*> & GetRows() const;
    const std::vector<std::string> & GetColumnNames() const;
    std::string GetName() const;
    std::string ToString(int n);
    Relation SelectWhere(const std::string &, const std::string &) const;
private:
    std::list<IRow*> rows;
    std::vector<std::string> columnNames;
    std::string name;
};


#endif //PROJECT2_RELATION_H
