//
// Created by fk on 07.07.22.
//

#ifndef PROJECT2_RELATION_H
#define PROJECT2_RELATION_H

#include <string>
#include <vector>
#include <array>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <algorithm>

// Improvement: Generic class with type list for columns?
template <int COLS>
class Relation {
public:
    explicit Relation(const std::string & name, const std::array<std::string,COLS> & columnNames, size_t size = 0) {
        this->name = name;
        this->columnNames = columnNames;
        if (size != 0) {
            this->rows.reserve(size);
        }
    }
    void AddRow(const std::array<std::string*, COLS>& row) {
        this->rows.push_back(row);
    };
    const std::vector<std::array<std::string *,COLS>> & GetRows() const {
        return this->rows;
    };
    const std::array<std::string,COLS> & GetColumnNames() const {
        return this->columnNames;
    }
    std::string GetName() const {
        return this->name;
    };
    std::string ToString(int n) {
        std::stringstream sstream;
        // HEADER
        sstream << this->name << std::endl;
        sstream << "|";
        for (auto const & col : this->columnNames)
            sstream << " " << std::left << std::setw(25) << col << " |";
        sstream << std::endl << "-";
        for (int i = 0; i < this->columnNames.size() * (25 + 3); i++)
            sstream << "-";
        // ROWS
        auto rowIterator = this->rows.begin();
        for (int i = 0; i < std::min(this->rows.size(), (size_t) n); i++) {
            sstream << std::endl << "|";
            auto const & cols = *rowIterator;
            for (auto const & col : cols) {
                sstream << " " << std::left << std::setw(25) << *col << " |";
            }
            sstream << std::endl << "-";
            for (int j = 0; j < this->columnNames.size() * (25 + 3); j++)
                sstream << "-";

            rowIterator++;
        }
        return sstream.str();
    };
    Relation SelectWhere(const std::string & columnName, const std::string & columnEntry) const {
        Relation output = Relation(this->name, this->columnNames);

        int idx;
        for (int i = 0; i < this->columnNames.size(); i++)
            if (this->columnNames[i] == columnName)
                idx = i;

        for (auto const & row : this->rows) {
            if (*(row[idx]) == columnEntry) {
                output.AddRow(row);
            }
        }

        return output;
    };
private:
    std::vector<std::array<std::string *, COLS>> rows;
    std::array<std::string, COLS> columnNames;
    std::string name;
};


#endif //PROJECT2_RELATION_H
