#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include "Relation.h"
#include "HashJoin.h"


std::unordered_map<std::string, Relation<2>*> getPartitionTablesFromRDF(const std::string& fileName) {
    // Idea: Multi-threaded reading of file
    std::ifstream ifstream(fileName);
    std::unordered_map<std::string, Relation<2>*> tables;

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
                auto result = tables.insert(
                        {tableName, new Relation<2>(tableName, {"Subject", "Object"})}
                );
                it = result.first;
            }
            auto * sub = new std::string(subject);
            auto * obj = new std::string(object);
            it->second->AddRow(std::array<std::string*,2> { sub, obj });
        }
    }
    ifstream.close();

    return tables;
}

int process100k() {
    auto const & tables = getPartitionTablesFromRDF("./100k.txt");

    auto const & followsRelation = tables.find("wsdbm:follows")->second;
    auto const & friendOfRelation = tables.find("wsdbm:friendOf")->second;

    std::cout << followsRelation->SelectWhere("Subject", "wsdbm:User1").ToString(5) << std::endl;
    std::cout << friendOfRelation->SelectWhere("Subject", "wsdbm:User123").ToString(5) << std::endl;

    HashJoin hashJoin = HashJoin<2, 2>();
//     auto joined = hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject");
//    std::cout << joined->ToString(5) << std::endl;
//    std::cout << joined->SelectWhere("Subject", "wsdbm:User1").ToString(5) << std::endl;
}

int processWatdiv10M() {
    auto const & tables = getPartitionTablesFromRDF("./watdiv.10M.nt");

    auto followsRelation = tables.find("<http://db.uwaterloo.ca/~galuc/wsdbm/follows>")->second;
    auto friendOfRelation = tables.find("<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>")->second;
    auto likesRelation = tables.find("<http://db.uwaterloo.ca/~galuc/wsdbm/likes>")->second;
    auto hasReviewRelation = tables.find("<http://purl.org/stuff/rev#hasReview>")->second;

    std::cout << "FollowsRelation: " << followsRelation->GetRows().size() << std::endl;
    std::cout << "FriendsOfRelation: " << friendOfRelation->GetRows().size() << std::endl;
    std::cout << "LikesRelation: " << likesRelation->GetRows().size() << std::endl;
    std::cout << "HasReviewRelation: " << hasReviewRelation->GetRows().size() << std::endl;

    auto * hashJoin = new HashJoin<2, 2>();
    auto joined = hashJoin->Join(likesRelation, "Object", hasReviewRelation, "Subject");
    int size = joined->GetRows().size();

    auto * hashJoin2 = new HashJoin<2, 3>();
    auto joined2 = hashJoin2->Join(friendOfRelation, "Object", joined, "Subject");
    size = joined2->GetRows().size();

    HashJoin hashJoin3 = HashJoin<2, 4>();
    auto joined3 = hashJoin3.Join(followsRelation, "Object", joined2, "Subject", true);
    std::cout << joined3->GetRows().size() << std::endl;

    /*
    HashJoin hashJoin = HashJoin<2, 2>();
    auto joined = hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject");
    std::cout << joined->ToString(5) << std::endl;
    HashJoin hashJoin2 = HashJoin<2, 3>();
    auto nextJoined = hashJoin2.Join(likesRelation, "Subject", joined, "Object");
    std::cout << nextJoined->ToString(5) << std::endl;
    */

    /*
    HashJoin hashJoin = HashJoin();
    auto joined = hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject");
    // std::cout << joined.SelectWhere("Subject", "<http://db.uwaterloo.ca/~galuc/wsdbm/User123>").ToString(5) << std::endl;
     */
}

bool test() {
    Relation<2> * relation1 = new Relation<2>("test1", {"a", "b"});

    std::string * row1a = new std::string("a1");
    std::string * row1b = new std::string("b1");
    std::array<std::string*, 2> row1 = {row1a, row1b};
    relation1->AddRow(row1);

    std::string * row2a = new std::string("a2");
    std::string * row2b = new std::string("b2");
    std::array<std::string*, 2> row2 = {row2a, row2b};
    relation1->AddRow(row2);

    Relation<2> * relation2 = new Relation<2>("test2", {"b", "c"});

    std::string * row3a = new std::string("b1");
    std::string * row3b = new std::string("c1");
    std::array<std::string*, 2> row3 = {row3a, row3b};
    relation2->AddRow(row3);

    std::string * row4a = new std::string("b1");
    std::string * row4b = new std::string("c2");
    std::array<std::string*, 2> row4 = {row4a, row4b};
    relation2->AddRow(row4);

    HashJoin<2,2> hashJoin = HashJoin<2,2>();
    auto joined = hashJoin.Join(relation1, "b", relation2, "b");
    std::cout << joined->ToString(2) << std::endl;


    return joined->GetRows().size() == 2;
};

int main() {
    std::cout << test() << std::endl;
    // process100k();
    // processWatdiv10M();
    return 0;
}
