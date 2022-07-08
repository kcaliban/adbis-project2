#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <sstream>
#include "Relation.h"
#include "HashJoin.h"

std::unordered_map<std::string, Relation> getPartitionTablesFromRDF(const std::string& fileName) {
    // Idea: Multi-threaded reading of file
    std::ifstream ifstream(fileName);
    std::unordered_map<std::string, Relation> tables;

    if (ifstream.is_open()) {
        std::string line;
        while (getline(ifstream, line)) {
            std::istringstream iss(line);
            std::string token;
            std::vector<std::string> tokens;
            while (getline(iss, token, '\t')) {
                tokens.push_back(token);
            }

            std::string subject = tokens[0], tableName = tokens[1], object = tokens[2];
            object = object.substr(0, object.length() - 2);

            auto it = tables.find(tableName);
            if (it == tables.end()) {
                auto result = tables.insert({tableName, Relation(tableName, {"Subject", "Object"})});
                it = result.first;
            }
            it->second.AddRow({subject, object});
        }
    }
    ifstream.close();

    return tables;
}

int process100k() {
    auto const & tables = getPartitionTablesFromRDF("./100k.txt");

    auto const & followsRelation = tables.find("wsdbm:follows")->second;
    auto const & friendOfRelation = tables.find("wsdbm:friendOf")->second;

    std::cout << followsRelation.SelectWhere("Subject", "wsdbm:User1").ToString(5) << std::endl;
    std::cout << friendOfRelation.SelectWhere("Subject", "wsdbm:User123").ToString(5) << std::endl;

    HashJoin hashJoin = HashJoin();
    auto joined = hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject");
    std::cout << joined.SelectWhere("Subject", "wsdbm:User1").ToString(5) << std::endl;
}

int processWatdiv10M() {
    auto const & tables = getPartitionTablesFromRDF("./watdiv.10M.nt");

    auto const & followsRelation = tables.find("<http://db.uwaterloo.ca/~galuc/wsdbm/follows>")->second;
    auto const & friendOfRelation = tables.find("<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>")->second;

    // std::cout << "Rows of followsRelation" << followsRelation.GetRows().size() << std::endl;
    // std::cout << "Rows of friendsOfRelatin" << friendOfRelation.GetRows().size() << std::endl;

    std::cout << followsRelation.SelectWhere("Subject", "<http://db.uwaterloo.ca/~galuc/wsdbm/User1>").ToString(5) << std::endl;
    std::cout << friendOfRelation.SelectWhere("Subject", "<http://db.uwaterloo.ca/~galuc/wsdbm/User123>").ToString(5) << std::endl;

    HashJoin hashJoin = HashJoin();
    auto joined = hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject");
    // std::cout << joined.SelectWhere("Subject", "<http://db.uwaterloo.ca/~galuc/wsdbm/User123>").ToString(5) << std::endl;
}

int main() {
    process100k();
    // processWatdiv10M();
    return 0;
}
