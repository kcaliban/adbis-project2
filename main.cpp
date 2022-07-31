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
                                                unsigned long long hashTableSize) {
    auto tableName = A->tableName + "_" + B->tableName;
    auto outputFileName = tableName + ".csv";
    auto outputFilePath = outputDir / std::filesystem::path(outputFileName);
    auto fstream = new std::fstream(outputFilePath, std::ios::out | std::ios::in | std::ios::trunc);
    HashJoin hashJoin(A, columnA, B, columnB,
                      fstream, hashTableSize);
    hashJoin.Join();
    fstream->seekg(0, std::ios::beg);
    return std::make_pair(
            new CSVReader(fstream, ',', tableName),
            fstream
    );
}

std::pair<CSVReader *, std::fstream *> sortMergeJoin(CSVReader * A, CSVReader * B, const std::string & columnA,
                                                     const std::string & columnB, const std::string & outputDir,
                                                     unsigned long long cacheSize, const std::string & tempDir,
                                                     bool multiThreaded = false) {
    auto tableName = A->tableName + "_" + B->tableName;
    auto outputFileName = tableName + ".csv";
    auto outputFilePath = outputDir / std::filesystem::path(outputFileName);
    auto fstream = new std::fstream(outputFilePath, std::ios::out | std::ios::in | std::ios::trunc);
    SortMergeJoin sortMergeJoin(A, columnA, B, columnB, fstream, cacheSize, tempDir);
    sortMergeJoin.Join(multiThreaded);
    fstream->seekg(0, std::ios::beg);
    return std::make_pair(
            new CSVReader(fstream, ',', tableName),
            fstream
    );
}

void clearDirectory(const std::filesystem::path & dir) {
    for (const auto& entry : std::filesystem::directory_iterator(dir))
        std::filesystem::remove_all(entry.path());
}

int process100k(std::ofstream & file) {
    unsigned long long cacheSize = 8 * pow(2, 30);
    unsigned long long hashMapSize = 8 * pow(2, 30);

    const auto inputFile = "100k.txt";
    const auto outputDir = "100k";
    const auto tempSubDir = "tmp";
    std::filesystem::path tempDir = std::filesystem::path(outputDir) / tempSubDir;
    std::filesystem::create_directories(tempDir);

    RDFReader(inputFile).ToPartitionedCSV(outputDir);

    auto follows = getCSVReader(outputDir, "wsdbm_follows.csv", "follows");
    auto friendOf = getCSVReader(outputDir, "wsdbm_friendOf.csv", "friendOf");
    auto likes = getCSVReader(outputDir, "wsdbm_likes.csv", "likes");
    auto hasReview = getCSVReader(outputDir, "rev_hasReview.csv", "hasReview");

    /* SMJ: SINGLE THREADED */
    auto outputDirSMJ = "SMJ";
    auto outputPathSMJ = std::filesystem::path(outputDir) / outputDirSMJ;
    std::filesystem::create_directories(outputPathSMJ);
    auto startSortMergeJoinST = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJST = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJST = sortMergeJoin(firstSMJST.first, likes.first, "friendOf.Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdSMJST = sortMergeJoin(secondSMJST.first, hasReview.first, "likes.Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    auto endSortMergeJoinST = std::chrono::steady_clock::now();

    std::cout << "\tSortMergeJoin (ST) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "[s]" << std::endl;

    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    // Close files
    firstSMJST.second->close();
    secondSMJST.second->close();
    thirdSMJST.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathSMJ);

    /* SMJ: MULTI THREADED */
    auto outputDirSMJMT = "SMJ_MT";
    auto outputPathSMJMT = std::filesystem::path(outputDir) / outputDirSMJMT;
    std::filesystem::create_directories(outputPathSMJMT);
    auto startSortMergeJoinMT = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJ = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJ = sortMergeJoin(firstSMJ.first, likes.first, "friendOf.Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdSMJ = sortMergeJoin(secondSMJ.first, hasReview.first, "likes.Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    auto endSortMergeJoinMT = std::chrono::steady_clock::now();

    std::cout << "\tSortMergeJoin (MT) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "[s]" << std::endl;

    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    // Close files
    firstSMJ.second->close();
    secondSMJ.second->close();
    thirdSMJ.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathSMJMT);

    /* HASHJOIN */
    auto outputDirHJ = "HJ";
    auto outputPathHJ = std::filesystem::path(outputDir) / outputDirHJ;
    std::filesystem::create_directories(outputPathHJ);
    auto starthashJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstHashJoin = hashJoin(follows.first, friendOf.first, "Object", "Subject", outputPathHJ, hashMapSize);
    std::cout << "JOIN likes" << std::endl;
    auto secondHashJoin = hashJoin(firstHashJoin.first, likes.first, "friendOf.Object", "Subject", outputPathHJ, hashMapSize);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdHashJoin = hashJoin(secondHashJoin.first, hasReview.first, "likes.Object", "Subject", outputPathHJ, hashMapSize);
    auto endhashJoin = std::chrono::steady_clock::now();

    std::cout << "-----------------------------" << std::endl;
    std::cout << "BENCHMARK RESULTS: " << std::endl;
    std::cout << "\tSortMergeJoin (ST) " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "\t";
    std::cout << "\tSortMergeJoin (MT) " << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::milliseconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "\t";
    std::cout << "\tHashJoin " << std::chrono::duration_cast<std::chrono::milliseconds>(endhashJoin - starthashJoin).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::milliseconds>(endhashJoin - starthashJoin).count() << "\t" << std::endl;

    // Close files
    firstHashJoin.second->close();
    secondHashJoin.second->close();
    thirdHashJoin.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathHJ);

    return 0;
}

int processWatdiv10M(std::ofstream & file) {
    unsigned long long cacheSize = 8 * pow(2, 30);
    unsigned long long hashMapSize = 8 * pow(2, 30);

    const auto inputFile = "watdiv.10M.nt";
    const auto outputDir = "watdiv_10M_nt";
    const auto tempSubDir = "tmp";
    std::filesystem::path tempDir = std::filesystem::path(outputDir) / tempSubDir;
    std::filesystem::create_directories(tempDir);

    RDFReader(inputFile).ToPartitionedCSV(outputDir);

    auto follows = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_follows.csv", "follows");
    auto friendOf = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_friendOf.csv", "friendOf");
    auto likes = getCSVReader(outputDir, "http___dbuwaterlooca_galuc_wsdbm_likes.csv", "likes");
    auto hasReview = getCSVReader(outputDir, "http___purlorg_stuff_rev:hasReview.csv", "hasReview");

    /* SMJ: SINGLE THREADED */
    auto outputDirSMJ = "SMJ";
    auto outputPathSMJ = std::filesystem::path(outputDir) / outputDirSMJ;
    std::filesystem::create_directories(outputPathSMJ);
    auto startSortMergeJoinST = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJST = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJST = sortMergeJoin(firstSMJST.first, likes.first, "friendOf.Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdSMJST = sortMergeJoin(secondSMJST.first, hasReview.first, "likes.Object", "Subject", outputPathSMJ, cacheSize, tempDir);
    auto endSortMergeJoinST = std::chrono::steady_clock::now();

    std::cout << "\tSortMergeJoin (ST) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "[s]" << std::endl;

    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    // Close files
    firstSMJST.second->close();
    secondSMJST.second->close();
    thirdSMJST.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathSMJ);

    /* SMJ: MULTI THREADED */
    auto outputDirSMJMT = "SMJ_MT";
    auto outputPathSMJMT = std::filesystem::path(outputDir) / outputDirSMJMT;
    std::filesystem::create_directories(outputPathSMJMT);
    auto startSortMergeJoinMT = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstSMJ = sortMergeJoin(follows.first, friendOf.first, "Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    std::cout << "JOIN likes" << std::endl;
    auto secondSMJ = sortMergeJoin(firstSMJ.first, likes.first, "friendOf.Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdSMJ = sortMergeJoin(secondSMJ.first, hasReview.first, "likes.Object", "Subject", outputPathSMJMT, cacheSize, tempDir, true);
    auto endSortMergeJoinMT = std::chrono::steady_clock::now();

    std::cout << "\tSortMergeJoin (MT) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "[s]" << std::endl;

    // Reset readers
    follows.first->JumpToBegin();
    friendOf.first->JumpToBegin();
    likes.first->JumpToBegin();
    hasReview.first->JumpToBegin();

    // Close files
    firstSMJ.second->close();
    secondSMJ.second->close();
    thirdSMJ.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathSMJMT);

    /* HASHJOIN */
    auto outputDirHJ = "HJ";
    auto outputPathHJ = std::filesystem::path(outputDir) / outputDirHJ;
    std::filesystem::create_directories(outputPathHJ);
    auto starthashJoin = std::chrono::steady_clock::now();
    std::cout << "follows JOIN friendOf" << std::endl;
    auto firstHashJoin = hashJoin(follows.first, friendOf.first, "Object", "Subject", outputPathHJ, hashMapSize);
    std::cout << "JOIN likes" << std::endl;
    auto secondHashJoin = hashJoin(firstHashJoin.first, likes.first, "friendOf.Object", "Subject", outputPathHJ, hashMapSize);
    std::cout << "JOIN hasReview" << std::endl;
    auto thirdHashJoin = hashJoin(secondHashJoin.first, hasReview.first, "likes.Object", "Subject", outputPathHJ, hashMapSize);
    auto endhashJoin = std::chrono::steady_clock::now();

    std::cout << "-----------------------------" << std::endl;
    std::cout << "BENCHMARK RESULTS: " << std::endl;
    std::cout << "\tSortMergeJoin (ST) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinST - startSortMergeJoinST).count() << "\t";
    std::cout << "\tSortMergeJoin (MT) " << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::seconds>(endSortMergeJoinMT - startSortMergeJoinMT).count() << "\t";
    std::cout << "\tHashJoin " << std::chrono::duration_cast<std::chrono::seconds>(endhashJoin - starthashJoin).count() << "[s]" << std::endl;
    file << std::chrono::duration_cast<std::chrono::seconds>(endhashJoin - starthashJoin).count() << "\t" << std::endl;

    // Close files
    firstHashJoin.second->close();
    secondHashJoin.second->close();
    thirdHashJoin.second->close();

    // Delete files
    clearDirectory(tempDir);
    std::filesystem::remove_all(outputPathHJ);

    return 0;
}

int main() {
    std::ofstream file("100k_bench");
    file << "SMJ\tSMJMT\tHJ" << std::endl;
    for (unsigned int i = 0; i < 50; i++)
        process100k(file);
    file.close();

    file = std::ofstream("watdiv_bench");
    file << "SMJ\tSMJMT\tHJ" << std::endl;
    for (unsigned int i = 0; i < 5; i++)
        processWatdiv10M(file);
    file.close();
}
