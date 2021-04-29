#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <condition_variable>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

void threadFunction(Network const& network, double alpha, uint32_t& iterations, double tolerance, std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
    std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks, std::unordered_set<PageId, PageIdHash>& danglingNodes,
    std::unordered_map<PageId, std::vector<PageId>, PageIdHash>& edges, std::mutex& mut, std::queue<const Page*>& queueOfPages,
    std::unordered_map<PageId, std::mutex*, PageIdHash>& mutexes, std::unordered_map<PageId, PageRank, PageIdHash>& newPageHashMap,
    std::vector<int32_t>& numberOfStillWorkingThreads, std::mutex& edgesMutex, std::vector<int32_t>& numberOfStillWorkingThreads2,
    std::condition_variable& synchronizer, double& difference, double& firstPart, double& dangleSum, double& newDangleSum,
    std::mutex& pageHashMapMutex, std::mutex& numLinksMutex, std::mutex& danglingNodesMutex, std::vector<std::mutex>& vectorOfMutexes, uint32_t& j)
{
    std::vector<const Page*> myPages;

    while (true) {
        mut.lock();
        if (queueOfPages.empty()) {
            mut.unlock();
            break;
        }
        auto page = queueOfPages.front();
        queueOfPages.pop();
        mut.unlock();
        myPages.push_back(page);
        page->generateId(network.getGenerator());
        edgesMutex.lock();
        edges[page->getId()] = std::vector<PageId>();
        edgesMutex.unlock();

        mut.lock();
        j--;
        mutexes[page->getId()] = &vectorOfMutexes[j];
        mut.unlock();

        pageHashMapMutex.lock();
        pageHashMap[page->getId()] = 1.0 / network.getSize();
        newPageHashMap[page->getId()] = 1.0 / network.getSize();
        pageHashMapMutex.unlock();
        numLinksMutex.lock();
        numLinks[page->getId()] = page->getLinks().size();
        numLinksMutex.unlock();
        if (page->getLinks().size() == 0) {
            danglingNodesMutex.lock();
            danglingNodes.insert(page->getId());
            danglingNodesMutex.unlock();
        }
    }

    mut.lock();
    numberOfStillWorkingThreads[iterations + 1]--;
    if (numberOfStillWorkingThreads[iterations + 1] > 0) {
        mut.unlock();
        std::unique_lock<std::mutex> lk(mut);
        synchronizer.wait(lk, [&] { return numberOfStillWorkingThreads[iterations + 1] == 0; });
    } else {
        mut.unlock();
        synchronizer.notify_all();
    }

    double myDangleSum = 0;
    for (auto& page : myPages) {
        if (page->getLinks().size() != 0) {
            for (auto link : page->getLinks()) {
                mutexes[link]->lock();
                edges[link].push_back(page->getId());
                mutexes[link]->unlock();
            }
        } else
            myDangleSum += pageHashMap[page->getId()];
    }

    mut.lock();
    dangleSum += myDangleSum;
    numberOfStillWorkingThreads[0]--;
    if (numberOfStillWorkingThreads[0] > 0) {
        mut.unlock();
        std::unique_lock<std::mutex> lk(mut);
        synchronizer.wait(lk, [&] { return numberOfStillWorkingThreads[0] == 0; });
    } else {
        firstPart = dangleSum * alpha * (1.0 / network.getSize()) + (1.0 - alpha) / network.getSize();
        mut.unlock();
        synchronizer.notify_all();
    }

    for (; iterations > 0;) {
        myDangleSum = 0;
        for (auto& page : myPages) {
            auto pageId = page->getId();
            auto& rank = newPageHashMap[pageId];
            auto previousRank = rank;
            rank = firstPart;
            if (edges.count(pageId) > 0) {
                for (auto link : edges[pageId]) {
                    rank += alpha * pageHashMap[link] / numLinks[link];
                }
            }
            if (page->getLinks().size() == 0) {
                myDangleSum += rank;
            }

            mut.lock();
            difference += std::abs(previousRank - rank);
            mut.unlock();
        }

        mut.lock();
        numberOfStillWorkingThreads[iterations]--;
        newDangleSum += myDangleSum;
        if (numberOfStillWorkingThreads[iterations] > 0) {
            uint32_t wsk = iterations;
            mut.unlock();
            std::unique_lock<std::mutex> lk(mut);

            synchronizer.wait(lk, [&] { return numberOfStillWorkingThreads[wsk] <= 0; });
        } else {
            mut.unlock();
            synchronizer.notify_all();
        }

        for (auto& page : myPages)
            pageHashMap[page->getId()] = newPageHashMap[page->getId()];

        mut.lock();
        numberOfStillWorkingThreads2[iterations]--;
        if (numberOfStillWorkingThreads2[iterations] > 0) {
            uint32_t wsk = iterations;
            mut.unlock();
            std::unique_lock<std::mutex> lk(mut);

            synchronizer.wait(lk, [&] { return numberOfStillWorkingThreads2[wsk] <= 0; });
        } else {
            if (difference < tolerance) {
                numberOfStillWorkingThreads2[iterations] = -1;
                mut.unlock();
                synchronizer.notify_all();
                break;
            }
            dangleSum = newDangleSum;
            newDangleSum = 0;

            firstPart = dangleSum * alpha * (1.0 / network.getSize()) + (1.0 - alpha) / network.getSize();
            difference = 0;
            iterations--;
            mut.unlock();
            synchronizer.notify_all();
        }

        mut.lock();
        if (numberOfStillWorkingThreads2[iterations] == -1) {
            mut.unlock();
            break;
        }

        mut.unlock();
    }
}

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::unordered_set<PageId, PageIdHash> danglingNodes;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        std::unordered_map<PageId, std::mutex*, PageIdHash> mutexes;
        std::mutex mut;
        std::queue<const Page*> queueOfPages;
        std::vector<std::mutex> vectorOfMutexes(network.getSize());
        uint32_t j = network.getSize();
        for (auto const& page : network.getPages()) {
            queueOfPages.push(&page);
        }

        std::queue<std::thread> threads;
        auto newPageHashMap = pageHashMap;
        std::vector<int32_t> numberOfStillWorkingThreads;
        std::vector<int32_t> numberOfStillWorkingThreads2;
        for (uint32_t i = 0; i < iterations + 10; ++i) {
            numberOfStillWorkingThreads.push_back(this->numThreads);
            numberOfStillWorkingThreads2.push_back(this->numThreads);
        }

        std::condition_variable synchronizer;
        double difference = 0;
        double firstPart = 0;
        std::mutex pageHashMapMutex, numLinksMutex, danglingNodesMutex, edgesMutex;
        double dangleSum = 0, newDangleSum = 0;

        for (uint32_t i = 0; i < numThreads; ++i) {
            threads.push(std::thread { threadFunction, std::ref(network), alpha, std::ref(iterations), tolerance, std::ref(pageHashMap), std::ref(numLinks), std::ref(danglingNodes),
                std::ref(edges), std::ref(mut), std::ref(queueOfPages), std::ref(mutexes), std::ref(newPageHashMap), std::ref(numberOfStillWorkingThreads), std::ref(edgesMutex),
                std::ref(numberOfStillWorkingThreads2), std::ref(synchronizer), std::ref(difference), std::ref(firstPart), std::ref(dangleSum), std::ref(newDangleSum),
                std::ref(pageHashMapMutex), std::ref(numLinksMutex), std::ref(danglingNodesMutex), std::ref(vectorOfMutexes), std::ref(j) });
        }

        while (!threads.empty()) {
            threads.front().join();
            threads.pop();
        }

        if (numberOfStillWorkingThreads2[iterations] == -1) {
            std::vector<PageIdAndRank> result;
            for (auto iter : pageHashMap)
                result.push_back(PageIdAndRank(iter.first, iter.second));

            ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);
            return result;
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
