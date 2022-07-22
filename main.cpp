#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <filesystem>
#include "src/HashJoin.h"
#include "src/CSVWriter.h"
#include "SortMergeJoin.h"
#include "RDFReader.h"
#include <cmath>
#include <chrono>

std::pair<CSVReader *, std::ifstream *> getCSVReader(const std::string& outputDir, const std::string& fileName,
                                                     const std::string& tableName) {
    auto path = std::filesystem::path(outputDir) / std::filesystem::path(fileName);
    auto ifstream = new std::ifstream(path);
    auto reader = new CSVReader(ifstream, ',', tableName);

    return std::make_pair(reader, ifstream);
}

std::pair<CSVReader *, std::fstream *> hashJoin(CSVReader * A, CSVReader * B, const std::string & columnA,
                                                const std::string & columnB, const std::string & outputDir,
                                                unsigned int cacheSize, unsigned int hashTableSize) {
    auto tableName = A->tableName + "_" + B->tableName;
    auto outputFileName = tableName + ".csv";
    auto outputFilePath = outputDir / std::filesystem::path(outputFileName);
    auto fstream = new std::fstream(outputFilePath, std::ios::out | std::ios::in | std::ios::trunc);
    HashJoin hashJoin(A, columnA, B, columnB,
                      fstream, cacheSize, hashTableSize);
    hashJoin.Join();
    std::cout << outputFilePath << std::endl;
    fstream->seekg(0, std::ios::beg);
    return std::make_pair(
            new CSVReader(fstream, ',', tableName),
            fstream
    );
}

std::pair<CSVReader *, std::fstream *> sortMergeJoin(CSVReader * A, CSVReader * B, const std::string & columnA,
                                                     const std::string & columnB, const std::string & outputDir,
                                                     unsigned int cacheSize, const std::string & tempDir) {
    auto tableName = A->tableName + "_" + B->tableName;
    auto outputFileName = tableName + ".csv";
    auto outputFilePath = outputDir / std::filesystem::path(outputFileName);
    auto fstream = new std::fstream(outputFilePath, std::ios::out | std::ios::in | std::ios::trunc);
    SortMergeJoin sortMergeJoin(A, columnA, B, columnB, fstream, cacheSize, tempDir);
    sortMergeJoin.Join();
    fstream->seekg(0, std::ios::beg);
    return std::make_pair(
            new CSVReader(fstream, ',', tableName),
            fstream
    );
}

int process100k() {
    unsigned int cacheSize = 2 * pow(2, 30);
    unsigned int hashMapSize = 2 * pow(2, 30);

    const auto inputFile = "100k.txt";
    const auto outputDir = "100k";
    std::filesystem::path tempDir = std::filesystem::path(outputDir) / "tmp";
    std::filesystem::create_directories(tempDir);

    RDFReader(inputFile).ToPartitionedCSV(outputDir, cacheSize);

    auto follows = getCSVReader(outputDir, "wsdbm_follows.csv", "follows");
    auto friendOf = getCSVReader(outputDir, "wsdbm_friendOf.csv", "friendOf");
    auto likes = getCSVReader(outputDir, "wsdbm_likes.csv", "likes");
    auto hasReview = getCSVReader(outputDir, "rev_hasReview.csv", "hasReview");

    auto startSortMergeJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJ = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJ = sortMergeJoin(firstSMJ.first, likes.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    std::cout << "JOIN hasReview" << std::endl;
    sortMergeJoin(secondSMJ.first, hasReview.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    auto endSortMergeJoin = std::chrono::steady_clock::now();

    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    auto starthashJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstHashJoin = hashJoin(follows.first, friendOf.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    std::cout << "JOIN likes" << std::endl;
    auto secondHashJoin = hashJoin(firstHashJoin.first, likes.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    std::cout << "JOIN hasReview" << std::endl;
    hashJoin(secondHashJoin.first, hasReview.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    auto endhashJoin = std::chrono::steady_clock::now();

    std::cout << "BENCHMARK RESULTS: " << std::endl;
    std::cout << "\tSortMergeJoin " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoin - startSortMergeJoin).count() << "[ms]" << std::endl;
    std::cout << "\tHashJoin " << std::chrono::duration_cast<std::chrono::milliseconds>(endhashJoin - starthashJoin).count() << "[ms]" << std::endl;


    /*
    auto first = hashJoin(follows.first, friendOf.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    auto second = hashJoin(first.first, likes.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    auto third = hashJoin(second.first, hasReview.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    */
}

int processWatdiv10M() {
    // 2GB
    unsigned int cacheSize = 2 * pow(2, 30);
    unsigned int hashMapSize = 2 * pow(2, 30);

    const auto inputFile = "watdiv.10M.nt";
    const auto outputDir = "watdiv_10M_nt";
    std::filesystem::path tempDir = std::filesystem::path(outputDir) / "tmp";
    std::filesystem::create_directories(tempDir);

    RDFReader(inputFile).ToPartitionedCSV(outputDir, cacheSize);

    auto follows = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_follows.csv", "follows");
    auto friendOf = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_friendOf.csv", "friendOf");
    auto likes = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_likes.csv", "likes");
    auto hasReview = getCSVReader(outputDir, "http___purlorg_stuff_rev:hasReview.csv", "hasReview");

    auto startSortMergeJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJ = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJ = sortMergeJoin(firstSMJ.first, likes.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    std::cout << "JOIN hasReview" << std::endl;
    sortMergeJoin(secondSMJ.first, hasReview.first, "Object", "Subject", outputDir, cacheSize, tempDir);
    auto endSortMergeJoin = std::chrono::steady_clock::now();

    std::cout << "BENCHMARK RESULTS: " << std::endl;
    std::cout << "\tSortMergeJoin " << std::chrono::duration_cast<std::chrono::minutes>(endSortMergeJoin - startSortMergeJoin).count() << "[m]" << std::endl;

    /*
    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    auto starthashJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstHashJoin = hashJoin(follows.first, friendOf.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    std::cout << "JOIN likes" << std::endl;
    auto secondHashJoin = hashJoin(firstHashJoin.first, likes.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    std::cout << "JOIN hasReview" << std::endl;
    hashJoin(secondHashJoin.first, hasReview.first, "Object", "Subject", outputDir, cacheSize, hashMapSize);
    auto endhashJoin = std::chrono::steady_clock::now();

    std::cout << "BENCHMARK RESULTS: " << std::endl;
    std::cout << "\tSortMergeJoin " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoin - startSortMergeJoin).count() << "[ms]" << std::endl;
    std::cout << "\tHashJoin " << std::chrono::duration_cast<std::chrono::milliseconds>(endhashJoin - starthashJoin).count() << "[ms]" << std::endl;

     */
}

int main() {

    // process100k();
    processWatdiv10M();
    /*
    unsigned int cacheSize = 70 * pow(2, 10);
    std::filesystem::path outputDir = "100k";
    std::filesystem::path tempDir = outputDir / "tmp";

    // getPartitionTablesFromRDF("100k.txt", outputDir, cacheSize);
    RDFReader("100k.txt").ToPartitionedCSV("100k", cacheSize);

    std::filesystem::create_directories(tempDir);

    auto followsRelationFileName = "wsdbm_follows.csv";
    auto followsRelationPath = std::filesystem::path("100k") / std::filesystem::path(followsRelationFileName);
    std::ifstream followsRelationStream(followsRelationPath);
    auto followsRelation = new CSVReader(&followsRelationStream, ',', "follows");

    auto friendOfRelationFileName = "wsdbm_friendOf.csv";
    auto friendOfRelationPath = std::filesystem::path("100k") / std::filesystem::path( friendOfRelationFileName);
    std::ifstream friendOfRelationStream(friendOfRelationPath);
    auto friendOfRelation = new CSVReader(&friendOfRelationStream, ',', "friendOf");

    std::ofstream outputFile(outputDir / "follows_friendof.csv");
    SortMergeJoin sortMergeJoin(followsRelation, "Object", friendOfRelation, "Subject", &outputFile, cacheSize, tempDir);
    sortMergeJoin.Join();

    return 0;
    */
 }

/*
    // Determine order of join (make sure that smaller ones are used for HashMap)
    std::vector<int> indices;
    for (int i = 0; i < tables.size(); i++)
        indices.push_back(i);

    std::sort(indices.begin(), indices.end(), [tables] (int i, int j) {
        return tables[i]->GetNumberOfRows() < tables[j]->GetNumberOfRows();
    });
*/