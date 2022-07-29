//
// Created by fk on 21.07.22.
//

#include "ExternalSortMultiThreaded.h"
#include "CSVWriter.h"
#include "Auxiliary.h"
#include <algorithm>
#include <random>
#include <iostream>
#include <thread>
#include <future>
#include <chrono>

/* THREAD FUNCTIONS */
void SortCacheThread(std::vector<std::vector<unsigned long>*> * threadCache, unsigned int columnIndex) {
    std::sort(threadCache->begin(), threadCache->end(),
              [columnIndex](std::vector<unsigned long> * a,
                            std::vector<unsigned long> * b) {
                  return a->at(columnIndex) < b->at(columnIndex);
              });
}

void MemMergeSortThread(std::vector<std::vector<unsigned long>*> * threadCacheA,
                        std::vector<std::vector<unsigned long>*> * threadCacheB,
                        std::vector<std::vector<unsigned long>*> * output,
                        unsigned int columnIndex) {
    auto itA = threadCacheA->begin();
    auto itB = threadCacheB->begin();

    while (itA != threadCacheA->end() && itB != threadCacheB->end()) {
        if ((*itA)->at(columnIndex) < (*itB)->at(columnIndex)) {
            output->push_back(*itA);
            itA++;
        } else if ((*itA)->at(columnIndex) > (*itB)->at(columnIndex)) {
            output->push_back(*itB);
            itB++;
        } else {
            output->push_back(*itA);
            output->push_back(*itB);
            itA++;
            itB++;
        }
    }

    while (itA != threadCacheA->end()) {
        output->push_back(*itA);
        itA++;
    }

    while (itB != threadCacheB->end()) {
        output->push_back(*itB);
        itB++;
    }
}

void MergeSortThread(const std::string& a, const std::string& b, const std::string& outputFile,
                     const std::filesystem::path tempDir, CSVReader * A, unsigned int columnIndex) {
    std::ofstream ostream(outputFile);
    std::ifstream istreamA(tempDir / a);
    std::ifstream istreamB(tempDir / b);

    CSVReader readerA(&istreamA, ',', "a");
    CSVReader readerB(&istreamB, ',', "b");
    CSVWriter writer(&ostream, A->columnNames, ',');

    auto rowA = readerA.GetNextRow();
    auto rowB = readerB.GetNextRow();

    while (!rowA.empty() && !rowB.empty()) {
        if (rowA[columnIndex] < rowB[columnIndex]) {
            writer.WriteNextRow(rowA);
            rowA = readerA.GetNextRow();
        } else if (rowA[columnIndex] > rowB[columnIndex]) {
            writer.WriteNextRow(rowB);
            rowB = readerB.GetNextRow();
        } else {
            writer.WriteNextRow(rowA);
            writer.WriteNextRow(rowB);
            rowA = readerA.GetNextRow();
            rowB = readerB.GetNextRow();
        }
    }

    // Write remaining rows
    while (!rowA.empty()) {
        writer.WriteNextRow(rowA);
        rowA = readerA.GetNextRow();
    }
    while (!rowB.empty()) {
        writer.WriteNextRow(rowB);
        rowB = readerB.GetNextRow();
    }

    ostream.close();
}

/* CLASS FUNCTIONS */
ExternalSortMultiThreaded::ExternalSortMultiThreaded(CSVReader *A, const std::string & column,
                           unsigned long long cacheSize, const std::filesystem::path & tempDir,
                           const std::filesystem::path & outputFilePath, unsigned int numThreads) {
    this->A = A;
    this->cacheSize = cacheSize;
    this->tempDir = tempDir;
    this->outputFilePath = outputFilePath;
    this->numThreads = numThreads;

    for (int i = 0; i < A->columnNames.size(); i++) {
        if (A->columnNames[i] == column)
            columnIndex = i;
    }
}

void ExternalSortMultiThreaded::Sort() {
    std::cout << "\t\t\tCREATE AND SORT CHUNKS" << std::endl;
    auto startChunks = std::chrono::steady_clock::now();
    CreateAndSortChunks();
    auto endChunks = std::chrono::steady_clock::now();
    std::cout << "\t\t\tFINISHED IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endChunks - startChunks).count() << "[ms]" << std::endl;

    std::cout << "\t\t\tMERGE CHUNKS" << std::endl;
    auto startMerge = std::chrono::steady_clock::now();
    KWayMergeSort();
    auto endMerge = std::chrono::steady_clock::now();
    std::cout << "\t\t\tFINISHED IN " << std::chrono::duration_cast<std::chrono::milliseconds>(endMerge - startMerge).count() << "[ms]" << std::endl;
}

void ExternalSortMultiThreaded::CreateAndSortChunks() {
    auto row = A->GetNextRow();
    auto cachePerThread = (unsigned long long) round(cacheSize / numThreads);

    while (!row.empty()) {
        if (caches.empty())
            caches.push_back(new std::vector<std::vector<unsigned long>*> ());
        auto lastThreadCache = caches.back();

        lastThreadCache->push_back(new std::vector<unsigned long>(row));
        if (caches.size() >= numThreads) {
            if (GetCachedSize(lastThreadCache->size()) < cachePerThread) {
                row = A->GetNextRow();
                continue;
            }

            std::vector<std::thread> threads;

            for (auto & threadCache : caches) {
                threads.emplace_back(SortCacheThread, threadCache, columnIndex);
            }

            for (auto & thread : threads)
                thread.join();

            KWayMergeSortMem();
        } else {
            if (GetCachedSize(lastThreadCache->size()) > cachePerThread) {
                caches.push_back(new std::vector<std::vector<unsigned long>*> ());
            }
        }
        row = A->GetNextRow();
    }

    std::vector<std::thread> threads;

    for (auto & threadCache : caches) {
        threads.emplace_back(SortCacheThread, threadCache, columnIndex);
    }

    for (auto & thread : threads)
        thread.join();

    KWayMergeSortMem();

    caches.shrink_to_fit();
}

void ExternalSortMultiThreaded::KWayMergeSortMem() {
    while (caches.size() > 1) {
        std::vector<std::vector<std::vector<unsigned long> *> *> newCaches;
        std::vector<std::pair<unsigned int, unsigned int>> mergePairs;

        for (int i = 0; i < caches.size() - 1; i += 2) {
            mergePairs.emplace_back(
                    std::make_pair(
                            i, i + 1
                    )

            );
        }

        std::vector<std::thread> mergeThreads;

        // Merge pairs into new caches.
        for (const auto & pair : mergePairs) {
            auto newCache = new std::vector<std::vector<unsigned long> *> ();
            std::thread thread(MemMergeSortThread, caches[pair.first],
                               caches[pair.second], newCache, columnIndex);
            mergeThreads.push_back(std::move(thread));
            newCaches.push_back(newCache);
        }

        for (auto & thread : mergeThreads)
            thread.join();

        // If number is uneven, last cache gets propagated
        if (caches.size() % 2 != 0) {
            newCaches.push_back(caches[caches.size() - 1]);
            caches.swap(newCaches);
            // Don't delete memory allocated for last element, as it is propagated
            for (unsigned int i = 0; i < newCaches.size() - 1; i++) {
                delete newCaches[i];
            }
            caches.shrink_to_fit();
            newCaches.shrink_to_fit();
        } else {
            caches.swap(newCaches);
            for (auto newCache : newCaches) {
                delete newCache;
            }
            caches.shrink_to_fit();
            newCaches.shrink_to_fit();
        }
    }

    std::string outputFileName = GetRandomString();
    std::filesystem::path output = tempDir / outputFileName;
    std::ofstream ofstream(output);

    CSVWriter writer(&ofstream, A->columnNames, ',');
    for (const auto row : *(caches[0])) {
        writer.WriteNextRow(*row);
    }

    ofstream.close();

    tempFiles.push_back(outputFileName);

    /* Clean caches and old rows */
    for (auto it : caches) {
        for (auto ptr : *it) {
            delete ptr;
        }
        delete it;
    }

    caches.clear();
    caches.shrink_to_fit();
}

unsigned long long ExternalSortMultiThreaded::GetCachedSize(unsigned long long elements) {
    // We need to reserve more space for the temporary created results of memory merging
    // and vector memory allocation in general, hence x1.5
    unsigned long long size = sizeof(std::vector<std::vector<unsigned long>*>) // Size of vector
                              + ((unsigned long long) round(elements * 1.5))
                                * sizeof(std::vector<unsigned long>*) // Size of pointers
                              + ((unsigned long long) round(elements * 1.5))
                                * sizeof(std::vector<unsigned long>) // Size of actual vectors
                              + ((unsigned long long) round(elements * 1.5))
                                * sizeof(unsigned long) * A->columnNames.size(); // Size of rows
    return size;
}

void ExternalSortMultiThreaded::KWayMergeSort() {
    while (tempFiles.size() > 1) {
        std::vector<std::string> newTempFiles;
        std::vector<std::pair<std::string,std::string>> mergePairs;
        for (int i = 0; i < tempFiles.size() - 1; i += 2) {
            mergePairs.emplace_back(
                    std::make_pair(
                            tempFiles[i], tempFiles[i + 1]
                    )

            );
        }

        // Merge pairs into new files
        for (const auto & pair : mergePairs) {
            auto mergedFileName = GetRandomString();
            auto mergedFilePath = tempDir / mergedFileName;


            MergeSort(pair.first, pair.second, mergedFilePath);
            newTempFiles.push_back(mergedFileName);
        }

        // If number is uneven, merge last merge result with remaining file
        if (tempFiles.size() % 2 != 0) {
            auto mergedFileName = GetRandomString();
            auto mergedFilePath = tempDir / mergedFileName;

            MergeSort(newTempFiles.back(), tempFiles.back(), mergedFilePath);

            auto tmpFile = newTempFiles.back();
            newTempFiles.pop_back();

            std::filesystem::path filePath = tempDir / tmpFile;
            std::filesystem::remove(filePath);

            newTempFiles.emplace_back(mergedFileName);
        }

        for (const auto & tmpFile : tempFiles) {
            std::filesystem::path filePath = tempDir / tmpFile;
            std::filesystem::remove(filePath);
        }

        // Set tempFiles = newTempFiles
        tempFiles = newTempFiles;
    }

    // Move file
    std::filesystem::rename(tempDir / tempFiles[0], outputFilePath);
}

void ExternalSortMultiThreaded::MergeSort(const std::string& a, const std::string& b, const std::string& outputFile) {
    std::ofstream ostream(outputFile);
    std::ifstream istreamA(tempDir / a);
    std::ifstream istreamB(tempDir / b);

    CSVReader readerA(&istreamA, ',', "a");
    CSVReader readerB(&istreamB, ',', "b");
    CSVWriter writer(&ostream, A->columnNames, ',');

    auto rowA = readerA.GetNextRow();
    auto rowB = readerB.GetNextRow();

    while (!rowA.empty() && !rowB.empty()) {
        if (rowA[columnIndex] < rowB[columnIndex]) {
            writer.WriteNextRow(rowA);
            rowA = readerA.GetNextRow();
        } else if (rowA[columnIndex] > rowB[columnIndex]) {
            writer.WriteNextRow(rowB);
            rowB = readerB.GetNextRow();
        } else {
            writer.WriteNextRow(rowA);
            writer.WriteNextRow(rowB);
            rowA = readerA.GetNextRow();
            rowB = readerB.GetNextRow();
        }
    }

    // Write remaining rows
    while (!rowA.empty()) {
        writer.WriteNextRow(rowA);
        rowA = readerA.GetNextRow();
    }
    while (!rowB.empty()) {
        writer.WriteNextRow(rowB);
        rowB = readerB.GetNextRow();
    }
}

/* SLOWER THAN SINGLE THREADED!
 * Either error in implementation (TODO: check)
 * Or multi-threaded file I/O slower than single-threaded
 *
void ExternalSortMultiThreaded::KWayMergeSort() {
    while (tempFiles.size() > 1) {
        std::vector<std::string> newTempFiles;
        std::vector<std::pair<std::string,std::string>> mergePairs;
        for (int i = 0; i < tempFiles.size() - 1; i += 2) {
            mergePairs.emplace_back(
                        std::make_pair(
                        tempFiles[i], tempFiles[i + 1]
                        )

            );
        }

        std::vector<std::thread> mergeThreads;

        // Merge pairs into new files
        for (const auto & pair : mergePairs) {
            auto mergedFileName = GetRandomString();
            auto mergedFilePath = tempDir / mergedFileName;
            newTempFiles.push_back(mergedFileName);

            mergeThreads.emplace_back(
                    std::thread(
                            [] (const std::string & a, const std::string & b, const std::string & out,
                                const std::filesystem::path & tempDir, CSVReader * A, unsigned int columnIndex) {
                                MergeSortThread(a, b, out, tempDir, A, columnIndex);
                            },
                            pair.first, pair.second, mergedFilePath, tempDir, A, columnIndex
                    )
            );
        }

        for (auto& thread : mergeThreads)
            thread.join();

        // If number is uneven, last cache gets propagated
        if (tempFiles.size() % 2 != 0) {
            newTempFiles.push_back(tempFiles.back());
            for (int i = 0; i < tempFiles.size() - 1; i++) {
                std::filesystem::path filePath = tempDir / tempFiles[i];
                std::filesystem::remove(filePath);
            }
        } else {
            for (const auto & tmpFile : tempFiles) {
                std::filesystem::path filePath = tempDir / tmpFile;
                std::filesystem::remove(filePath);
            }
        }

        tempFiles = newTempFiles;
    }

    // Move file
    std::filesystem::rename(tempDir / tempFiles[0], outputFilePath);
}
*/
