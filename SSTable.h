//
// Created by LEinfinitr on 2023/3/14.
//

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H

#include "utils.h"
#include "skiplist.h"
#include <iostream>
#include <fstream>
#include <vector>

struct Header{
    uint64_t time_stamp;
    uint64_t num_KVPair;
    uint64_t max_key;
    uint64_t min_key;
};

// SSTable 需要缓存的内容
struct Cache_content{
    Header header;
    bool bloom_filter[81920] = {false};
    std::vector<std::pair<uint64_t, uint32_t>> index_area;
};

class SSTable{
public:
    uint64_t time_stamp = 0;    // 时间戳
    std::vector<Cache_content> cache_area;  // 存储所有 SStable 缓存内容的缓存区

    void createBloomFilter(const Skiplist &mem_table, bool (&bloom_filter)[81920]);
    void writeToDisk(const Skiplist &mem_table, bool (&bloom_filter)[81920], const std::string &file_name);
    void addToCacheArea(const Skiplist &mem_table, bool (&bloom_filter)[81920]);
};

#endif //LSMKV_SSTABLE_H
