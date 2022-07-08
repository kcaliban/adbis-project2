//
// Created by fk on 07.07.22.
//

#ifndef PROJECT2_RELATION_H
#define PROJECT2_RELATION_H


#include <string>
#include <vector>

// Improvement: Generic class with type list for columns?
class Relation {
public:
    explicit Relation(const std::string&, const std::vector<std::string>&);
    virtual ~Relation();
    void AddRow(const std::vector<std::string>*);
    const std::vector<const std::vector<std::string>*> & GetRows() const;
    const std::vector<std::string> & GetColumnNames() const;
    std::string GetName() const;
    std::string ToString(int n);
    Relation SelectWhere(const std::string &, const std::string &) const;
private:
    std::vector<const std::vector<std::string>*> rows;
    std::vector<std::string> columnNames;
    std::string name;
};


#endif //PROJECT2_RELATION_H
