//
// Created by LEinfinitr on 2023/3/14.
//
// 用于保存所有文件的缓存信息、时间戳和实现 SSTable 相关处理的类
// 1.每个文件名为 file + 时间戳 + .sst，因此每个文件的时间戳均不相同
// 2.时间戳均为非负整数，且时间戳越大，文件越新

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H

#include "utils.h"
#include "skiplist.h"
#include <iostream>
#include <fstream>
#include <vector>

struct Header {
    uint64_t time_stamp;
    uint64_t num_KVPair;
    uint64_t max_key;
    uint64_t min_key;
};

// SSTable 需要缓存的内容
struct Cache_content {
    Header header;
    bool bloom_filter[10240];
    std::vector<std::pair<uint64_t, uint32_t>> index_area;
    int level;  // 该 SSTable 在哪一层
};

class SSTable {
private:
    void addToCacheArea(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level, uint64_t given_time_stamp);

    void writeToDisk(const Skiplist &mem_table, bool (&bloom_filter)[10240], const std::string &file_name,
                     uint64_t given_time_stamp);

public:
    std::string dir_name;   // 该 SSTable 所在的文件夹名
    uint64_t time_stamp = 1;    // 时间戳,只能通过 writeToSSTable() 函数修改
    std::vector<Cache_content> cache_area;  // 存储所有 SStable 缓存内容的缓存区

    bool search(uint64_t search_key, std::string &insert_value);

    void reset();

    bool bloomFilter(uint64_t search_key, bool (&bloom_filter)[10240]);

    uint32_t binarySearch(const std::vector<std::pair<uint64_t, uint32_t>> &index_area, uint64_t search_key);

    void createBloomFilter(const Skiplist &mem_table, bool (&bloom_filter)[10240]);

    void readToCacheArea(const std::string &file_name);

    void writeToSSTable(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level);

    void writeToSSTable(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level, uint64_t given_time_stamp);

    void readFromDisk(std::vector<std::pair<uint64_t, std::string>> &kvs, Cache_content &cache_content);

    void deleteFile(Cache_content &cache_content);
};

#endif //LSMKV_SSTABLE_H
