//
// Created by LEinfinitr on 2023/3/14.
//

#include "SSTable.h"
#include "MurmurHash3.h"
#include <string>

// 根据 mem_table 创建 Bloom Filter
void SSTable::createBloomFilter(const Skiplist &mem_table, bool (&bloom_filter)[81920])
{
    SkiplistNode* head = mem_table.head;
    while (head->below) head = head->below;
    if(head->value.second.empty()) head = head->next;
    while (head){
        unsigned int hash_res[4] = {0};
        MurmurHash3_x64_128 (&head->value.second, head->value.second.size(), 1, hash_res);
        uint32_t h1, h2, h3, h4;
        h1 = (((uint32_t*)hash_res)[0]) % 81920;
        h2 = ((uint32_t*)hash_res)[1] % 81920;
        h3 = ((uint32_t*)hash_res)[2] % 81920;
        h4 = ((uint32_t*)hash_res)[3] % 81920;
        bloom_filter[h1] = true;
        bloom_filter[h2] = true;
        bloom_filter[h3] = true;
        bloom_filter[h4] = true;
        head = head->next;
    }
}

// 向已经创建好的文件中按照一定的格式存储数据
void SSTable::writeToDisk(const Skiplist &mem_table, bool (&bloom_filter)[81920], const std::string &file_name)
{
    std::ofstream out(file_name);
    // 保存 Header
    out << time_stamp << " " << mem_table.count << " " << mem_table.min_key << " " << mem_table.max_key << std::endl;

    // 保存 Bloom Filter
    for(auto i : bloom_filter)
        out << i;
    out << std::endl;

    // 保存索引区
    SkiplistNode* head = mem_table.head;
    while (head->below) head = head->below;
    if(head->value.second.empty()) head = head->next;
    uint32_t offset = 0;
    while (head){
        out << head->value.first << " " << offset << " ";
        offset++;
        head = head->next;
    }

    // 保存数据区
    head = mem_table.head;
    while (head->below) head = head->below;
    if(head->value.second.empty()) head = head->next;
    while (head){
        out << head->value.second << " ";
        head = head->next;
    }

    out.close();
}

// 将 men_table 中需要缓存的信息存入缓存区
void SSTable::addToCacheArea(const Skiplist &mem_table, bool (&bloom_filter)[81920])
{
    Cache_content cache_information;
    // 缓存 Header
    cache_information.header.time_stamp = time_stamp;
    cache_information.header.num_KVPair = mem_table.count;
    cache_information.header.max_key = mem_table.max_key;
    cache_information.header.min_key = mem_table.min_key;

    // 缓存 Bloom Filter
    for (int i = 0; i < 81920; i++)
        cache_information.bloom_filter[i] = bloom_filter[i];

    // 缓存索引区
    SkiplistNode* head = mem_table.head;
    while (head->below) head = head->below;
    if(head->value.second.empty()) head = head->next;
    uint32_t offset = 0;
    while (head){
        cache_information.index_area.emplace_back(std::make_pair(head->value.first, offset));
        offset++;
        head = head->next;
    }

    cache_area.emplace_back(cache_information);
}