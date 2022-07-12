#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <filesystem>
#include "HashJoin.h"
#include "CSVWriter.h"
#include <cmath>

std::string replaceString(std::string subject,
                          const std::string& search,
                          const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
        subject.replace(pos, search.length(), replace);
        pos += replace.length();
    }
    return subject;
}

void getPartitionTablesFromRDF(const std::string& fileName, const std::string& outputDir, unsigned int cacheSize) {
    std::filesystem::create_directories(outputDir);

    std::ifstream ifstream(fileName);
    std::unordered_map<std::string, CSVWriter*> tables;

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

            // Get rid of special symbols in table name (used as the ouput file name) for watdiv.10M.nt
            tableName = replaceString(tableName, "<", "");
            tableName = replaceString(tableName, ">", "");
            tableName = replaceString(tableName, ".", "");
            tableName = replaceString(tableName, "~", "");
            tableName = replaceString(tableName, ":", "_");
            tableName = replaceString(tableName, "#", ":");
            tableName = replaceString(tableName, "/", "_");

            auto it = tables.find(tableName);
            if (it == tables.end()) {
                auto outputFile = std::filesystem::path(outputDir) / std::filesystem::path(tableName + ".csv");
                auto result = tables.insert(
                        {tableName, new CSVWriter( outputFile, {"Subject", "Object"}, ',', cacheSize)}
                );
                it = result.first;
            }
            auto sub = std::string(subject);
            auto obj = std::string(object);
            it->second->WriteNextRow({sub, obj});
        }
    }
    ifstream.close();

    for (auto & it : tables) {
        delete it.second;
    }
}


/*
int process100k() {
    // 4 GB Cache size
    unsigned int cacheSize = pow(2, 35);

    getPartitionTablesFromRDF("100k.txt", "100k", cacheSize);

    auto followsRelationFileName = "wsdbm:follows.csv";
    auto followsRelationPath = std::filesystem::path("100k") / std::filesystem::path(followsRelationFileName);
    auto followsRelation = new CSVReader(followsRelationPath, ',', "follows");

    auto friendOfRelationFileName = "wsdbm:friendOf.csv";
    auto friendOfRelationPath = std::filesystem::path("100k") / std::filesystem::path( friendOfRelationFileName);
    auto friendOfRelation = new CSVReader(friendOfRelationPath, ',', "friendOf");

    auto likesRelationFileName = "wsdbm:likes.csv";
    auto likesRelationPath = std::filesystem::path("100k") / std::filesystem::path(likesRelationFileName);
    auto likesRelation = new CSVReader(likesRelationPath, ',', "likes");

    auto hasReviewFileName = "rev:hasReview.csv";
    auto hasReviewPath = std::filesystem::path("100k") / std::filesystem::path(hasReviewFileName);
    auto hasReviewRelation = new CSVReader(hasReviewPath, ',', "hasReview");

    // follows JOIN friendOf
    auto outputFileName = "follows_friendOf.csv";
    auto outputFilePath = std::filesystem::path("100k") / std::filesystem::path(outputFileName);
    HashJoin hashJoin = HashJoin();
    hashJoin.Join(followsRelation, "Object", friendOfRelation, "Subject", outputFilePath, cacheSize);
    auto follows_friendOf = new CSVReader(outputFilePath, ',', "follows_friendOf");
    hashJoin.Reset();

    // JOIN likes
    outputFileName = "follows_friendOf_likes.csv";
    outputFilePath = std::filesystem::path("100k") / std::filesystem::path(outputFileName);
    hashJoin.Join(follows_friendOf, "Object", likesRelation, "Subject", outputFilePath, cacheSize);
    auto follows_friendOf_likes = new CSVReader(outputFilePath, ',', "follows_friendOf_likes");
    hashJoin.Reset();

    // JOIN hasReview
    outputFileName = "follows_friendOf_likes_hasReview.csv";
    outputFilePath = std::filesystem::path("100k") / std::filesystem::path(outputFileName);
    hashJoin.Join(follows_friendOf_likes, "Object", hasReviewRelation, "Subject", outputFilePath, cacheSize);
    auto follows_friendOf_likes_hasReview = new CSVReader(outputFilePath, ',', "follows_friendOf_likes");
    hashJoin.Reset();
}
 */

int processWatdiv10M() {
    // 4GB
    unsigned int cacheSize = 2 * pow(2, 30);
    // 4GB
    unsigned int hashMapSize = 2 * pow(2, 30);

    getPartitionTablesFromRDF("watdiv.10M.nt", "watdiv_10M_nt", cacheSize);

    auto followsRelationFileName = "http___dbuwaterlooca_galuc_wsdbm_follows.csv";
    auto followsRelationPath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(followsRelationFileName);
    auto followsRelation = new CSVReader(followsRelationPath, ',', "follows");

    auto friendOfRelationFileName = "http___dbuwaterlooca_galuc_wsdbm_friendOf.csv";
    auto friendOfRelationPath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path( friendOfRelationFileName);
    auto friendOfRelation = new CSVReader(friendOfRelationPath, ',', "friendOf");

    auto likesRelationFileName = "http___dbuwaterlooca_galuc_wsdbm_likes.csv";
    auto likesRelationPath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(likesRelationFileName);
    auto likesRelation = new CSVReader(likesRelationPath, ',', "likes");

    auto hasReviewFileName = "http___purlorg_stuff_rev:hasReview.csv";
    auto hasReviewPath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(hasReviewFileName);
    auto hasReviewRelation = new CSVReader(hasReviewPath, ',', "hasReview");

    // hasReview JOIN likes
    auto outputFileName = "hasReview_likes.csv";
    auto outputFilePath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(outputFileName);
    auto hashJoin = new HashJoin(hasReviewRelation, "Subject", likesRelation, "Object", outputFilePath, cacheSize);
    hashJoin->Join(hashMapSize);
    auto hasReview_likes = new CSVReader(outputFilePath, ',', "hasReview_likes");
    delete hashJoin;

    // JOIN friendOf
    outputFileName = "hasReview_likes_friendOf.csv";
    outputFilePath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(outputFileName);
    hashJoin = new HashJoin(hasReview_likes, "Subject", friendOfRelation, "Object", outputFilePath, cacheSize);
    hashJoin->Join(hashMapSize);
    auto hasReview_likes_friendOf = new CSVReader(outputFilePath, ',', "hasReview_likes_friendOf");
    delete hashJoin;

    // JOIN follows
    outputFileName = "hasReview_likes_friendOf_follows.csv";
    outputFilePath = std::filesystem::path("watdiv_10M_nt") / std::filesystem::path(outputFileName);
    hashJoin = new HashJoin(hasReview_likes_friendOf, "Subject", followsRelation, "Object", outputFilePath, cacheSize);
    hashJoin->Join(hashMapSize);
    auto hasReview_likes_friendOf_follows = new CSVReader(outputFilePath, ',', "hasReview_likes_friendOf_follows");
    delete hashJoin;
}




int main() {
    //process100k();
    processWatdiv10M();

    return 0;
}

/*
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
*/