#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "SSTable.h"
#include "utils.h"
#include <string>

class KVStore : public KVStoreAPI {
public:
    Skiplist memTable;
    SSTable disk;
    std::string dir;
    uint64_t file_num_level = 2; // 第 level 最大文件数量为 file_num_per_level * (2 ^ level)

    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    void compaction(int level);

    void getCacheInfo(std::vector<uint64_t> &res, int level);

    void mergeSort(std::vector<std::vector<std::pair<uint64_t, std::string>>> &kvs,
                   std::vector<std::pair<uint64_t, std::string>> &ret);

    void writeToDisk(std::vector<std::pair<uint64_t, std::string>> &kvs, int level, std::vector<uint64_t> &time_stamps);

    void analysis(uint64_t key);

    void printFileLevel();
};
