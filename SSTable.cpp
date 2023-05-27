//
// Created by LEinfinitr on 2023/3/14.
//

#include "SSTable.h"
#include "MurmurHash3.h"
#include <string>

// 在 SStable 中查找键值对
// 若查找到多条记录，使用 time_stamp 最大的记录
// 如果查找到记录返回 true 并将结果存储在 search_value 中
// 如果未查找到记录或查找到删除标记，则返回 false
bool SSTable::search(uint64_t search_key, std::string &search_value) {
    // 存储查找到的所有记录和对应的时间戳
    std::vector<std::pair<std::string, uint64_t>> search_res;
    // 遍历所有缓存区
    for (auto &i: cache_area) {
        // 如果查找的键落入该缓存区的键值范围内
        if (search_key >= i.header.min_key && search_key <= i.header.max_key) {
            // 如果 Bloom Filter 判断该键值对可能在缓存区中
            if (bloomFilter(search_key, i.bloom_filter)) {
                // 从索引区中查找该键值对
                uint32_t offset = binarySearch(i.index_area, search_key);
                if (offset != 0) {
                    // 从磁盘中读取数据
                    std::ifstream in;
                    in.open(dir_name + "/level" + std::to_string(i.level) + "/file" +
                            std::to_string(i.header.time_stamp) +
                            ".sst",
                            std::ios::in | std::ios::binary);
                    in.seekg(offset);
                    getline(in, search_value);
                    in.close();
                    // 将该键值对和时间戳存入 search_res
                    search_res.emplace_back(search_value, i.header.time_stamp);
                }
            }
        }
    }

    /*
    // 内存中没有缓存 SSTable 的任何信息，从磁盘中访问 SSTable 的索引，在找到 offset 之后读取数据
    // 得到 dir_name 目录下的所有 level 的文件路径
    std::vector<std::string> file_list;
    uint64_t level = 0;
    while (utils::dirExists(dir_name + "/level" + std::to_string(level))) {
        std::vector<std::string> tmp;
        utils::scanDir(dir_name + "/level" + std::to_string(level), tmp);
        for(const auto& file_name : tmp)
            file_list.emplace_back(dir_name + "/level" + std::to_string(level) + "/" + file_name);
        level++;
    }
    // 遍历所有文件
    for (auto &file_name: file_list) {
        // 读入文件信息和索引区
        std::ifstream in(file_name, std::ios::in | std::ios::binary);
        Cache_content cache_information;
        // 读取 Header
        in.read(reinterpret_cast<char *>(&cache_information.header), 32);

        // 读取 Bloom Filter
        for (bool &j: cache_information.bloom_filter)
            in.read(reinterpret_cast<char *>(&j), 1);

        // 读取索引区
        for (int i = 0; i < cache_information.header.num_KVPair; i++) {
            uint64_t key;
            uint32_t offset;
            in.read(reinterpret_cast<char *>(&key), 8);
            in.read(reinterpret_cast<char *>(&offset), 4);
            cache_information.index_area.emplace_back(key, offset);
        }

        in.close();
        // 如果查找的键落入该文件的键值范围内
        if (search_key >= cache_information.header.min_key && search_key <= cache_information.header.max_key) {
            // 遍历索引区找到 search_key 对应的偏移量
            uint32_t offset = binarySearch(cache_information.index_area, search_key);
            if (offset != 0) {
                // 从磁盘中读取数据
                std::ifstream in_data;
                in_data.open(file_name, std::ios::in | std::ios::binary);
                in_data.seekg(offset);
                getline(in_data, search_value);
                in_data.close();
                // 将该键值对和时间戳存入 search_res
                search_res.emplace_back(search_value, cache_information.header.time_stamp);
            }
        }
    }*/

    // 如果 search_res 不为空
    if (!search_res.empty()) {
        // 找到时间戳最大的键值对
        uint64_t max_time_stamp = 0;
        for (auto &i: search_res) {
            if (i.second > max_time_stamp) {
                max_time_stamp = i.second;
                search_value = i.first;
            }
        }
        // 如果为删除标记则返回 false
        if (search_value == "~DELETED~") return false;
        return true;
    }
    return false;
}

// 重置 SSTable
void SSTable::reset() {
    time_stamp = 1;
    cache_area.clear();
}

// 通过 Bloom Filter 判断 key 是否在文件中
bool SSTable::bloomFilter(uint64_t search_key, bool (&bloom_filter)[10240]) {
    unsigned int hash_res[4] = {0};
    MurmurHash3_x64_128(&search_key, 8, 1, hash_res);
    uint32_t h1, h2, h3, h4;
    h1 = (((uint32_t *) hash_res)[0]) % 10240;
    h2 = ((uint32_t *) hash_res)[1] % 10240;
    h3 = ((uint32_t *) hash_res)[2] % 10240;
    h4 = ((uint32_t *) hash_res)[3] % 10240;
    if (bloom_filter[h1] && bloom_filter[h2] && bloom_filter[h3] && bloom_filter[h4]) return true;
    return false;
}

//    使用二分查找法在索引区中查找 key, 返回 key 对应的 value 在文件中的偏移量
//    如果未查找到则返回 0
uint32_t SSTable::binarySearch(const std::vector<std::pair<uint64_t, uint32_t>> &index_area, uint64_t search_key) {
    uint32_t left = 0, right = index_area.size() - 1;
    while (left <= right) {
        uint32_t mid = (left + right) / 2;
        if (index_area[mid].first == search_key) return index_area[mid].second;
        else if (index_area[mid].first < search_key) left = mid + 1;
        else right = mid - 1;
    }
    return 0;
}

// 根据 mem_table 创建 Bloom Filter
void SSTable::createBloomFilter(const Skiplist &mem_table, bool (&bloom_filter)[10240]) {
    // 初始化 Bloom Filter
    for (auto &i: bloom_filter) i = false;
    SkiplistNode *head = mem_table.head;
    while (head->below) head = head->below;
    if (head->value.second.empty()) head = head->next;
    while (head) {
        unsigned int hash_res[4] = {0};
        MurmurHash3_x64_128(&head->value.first, 8, 1, hash_res);
        uint32_t h1, h2, h3, h4;
        h1 = ((uint32_t *) hash_res)[0] % 10240;
        h2 = ((uint32_t *) hash_res)[1] % 10240;
        h3 = ((uint32_t *) hash_res)[2] % 10240;
        h4 = ((uint32_t *) hash_res)[3] % 10240;
        bloom_filter[h1] = true;
        bloom_filter[h2] = true;
        bloom_filter[h3] = true;
        bloom_filter[h4] = true;
        head = head->next;
    }
}

// 向文件中按照固定的格式存储 SSTable 数据
// 在每个数据的结尾加上 \n 便于读取
// 不可直接调用，只能通过 writeToSSTable 函数和 addToCacheArea 同时使用
void SSTable::writeToDisk(const Skiplist &mem_table, bool (&bloom_filter)[10240], const std::string &file_name,
                          uint64_t given_time_stamp) {
    std::ofstream out(file_name, std::ios::binary | std::ios::out);
    // 按照固定字节大小保存 Header
    out.write(reinterpret_cast<const char *>(&given_time_stamp), 8);
    out.write(reinterpret_cast<const char *>(&mem_table.count), 8);
    out.write(reinterpret_cast<const char *>(&mem_table.max_key), 8);
    out.write(reinterpret_cast<const char *>(&mem_table.min_key), 8);

    // 保存 Bloom Filter
    for (auto i: bloom_filter)
        out.write(reinterpret_cast<const char *>(&i), 1);

    // 定位到第一个开始输出数据的位置
    SkiplistNode *head = mem_table.head;
    while (head->below) head = head->below; // 定位到最底层
    if (head->value.second.empty()) head = head->next;  // 若 key 为 0 的节点数据为空，则跳过
    SkiplistNode *tmp = head;   // 保存当前节点

    // 保存索引区
    uint32_t offset = 10272 + mem_table.count * 12;
    while (head) {
        out.write(reinterpret_cast<const char *>(&head->value.first), 8);
        out.write(reinterpret_cast<const char *>(&offset), 4);
        offset += head->value.second.size() + 1;
        head = head->next;
    }

    // 保存数据区
    head = tmp;
    while (head) {
        // 由于数值均为string，故可以不使用二进制输出，也不会影响偏移量的定位
        // 使用格式化输出并在数据末尾加上 \n 便于读取
        out << head->value.second << "\n";
        head = head->next;
    }

    out.close();
}

// 将文件中需要缓存的部分读入缓存区
void SSTable::readToCacheArea(const std::string &file_name) {
    std::ifstream in(file_name, std::ios::binary | std::ios::in);
    Cache_content cache_information;

    // 从文件名中解析得到 level
    // 文件名格式为 .../data/levelX/fileX.sst
    std::string level_str = file_name.substr(file_name.find("level") + 5, 1);
    cache_information.level = std::stoi(level_str);

    // 读取 Header
    in.read(reinterpret_cast<char *>(&cache_information.header), 32);

    // 读取 Bloom Filter
    for (bool &i: cache_information.bloom_filter)
        in.read(reinterpret_cast<char *>(&i), 1);

    // 读取索引区
    for (int i = 0; i < cache_information.header.num_KVPair; i++) {
        uint64_t key;
        uint32_t offset;
        in.read(reinterpret_cast<char *>(&key), 8);
        in.read(reinterpret_cast<char *>(&offset), 4);
        cache_information.index_area.emplace_back(key, offset);
    }

    in.close();
    cache_area.push_back(cache_information);
}

// 将 men_table 中需要缓存的信息存入缓存区
// 不可直接调用，只能通过 writeToSSTable 函数和 writeToDisk 同时使用
void
SSTable::addToCacheArea(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level, uint64_t given_time_stamp) {
    Cache_content cache_information;
    // 缓存 Header
    cache_information.header.time_stamp = given_time_stamp;
    cache_information.header.num_KVPair = mem_table.count;
    cache_information.header.max_key = mem_table.max_key;
    cache_information.header.min_key = mem_table.min_key;

    // 缓存 level
    cache_information.level = level;

    // 缓存 Bloom Filter
    for (int i = 0; i < 10240; i++)
        cache_information.bloom_filter[i] = bloom_filter[i];

    // 缓存索引区
    SkiplistNode *head = mem_table.head;
    while (head->below) head = head->below;
    if (head->value.second.empty()) head = head->next;
    uint32_t offset = sizeof(Header) + 10240 + mem_table.count * 12;
    while (head) {
        cache_information.index_area.emplace_back(head->value.first, offset);
        offset += head->value.second.size() + 1;
        head = head->next;
    }

    cache_area.emplace_back(cache_information);
}

// 向 SSTable 的第 level 层写入数据, 并将数据存入缓存区
// 使用最新的 time_stamp, 同时自动完成 time_stamp 的更新
// time_stamp 也仅在此函数中更新
void SSTable::writeToSSTable(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level) {
    addToCacheArea(mem_table, bloom_filter, level, time_stamp);
    writeToDisk(mem_table, bloom_filter,
                dir_name + "/level" + std::to_string(level) + "/file" + std::to_string(time_stamp) + ".sst",
                time_stamp);
    time_stamp++;
}

// 向 SSTable 的第 level 层写入数据, 并将数据存入缓存区
// 使用指定的 time_stamp
void
SSTable::writeToSSTable(const Skiplist &mem_table, bool (&bloom_filter)[10240], int level, uint64_t given_time_stamp) {
    addToCacheArea(mem_table, bloom_filter, level, given_time_stamp);
    writeToDisk(mem_table, bloom_filter,
                dir_name + "/level" + std::to_string(level) + "/file" + std::to_string(given_time_stamp) + ".sst",
                given_time_stamp);
}

// 根据 cache_content 中的信息，将文件中所有的键值对读入 kvs 中(在 kvs 末尾添加)
void SSTable::readFromDisk(std::vector<std::pair<uint64_t, std::string>> &kvs, Cache_content &cache_content) {
    // 根据 level 和 time_stamp 得到文件名
    std::string file_name =
            "data/level" + std::to_string(cache_content.level) + "/file" +
            std::to_string(cache_content.header.time_stamp) +
            ".sst";
    std::ifstream in(file_name, std::ios::binary | std::ios::in);

    // 根据索引区读取数据区
    for (auto i: cache_content.index_area) {
        in.seekg(i.second);
        std::string value;
        std::getline(in, value);
        kvs.emplace_back(i.first, value);
    }

    in.close();
}

// 根据 cache_content 中的信息，删除文件和缓存区中的信息
void SSTable::deleteFile(Cache_content &cache_content) {
    // 根据 level 和 time_stamp 得到文件名
    std::string file_name =
            dir_name + "/level" + std::to_string(cache_content.level) + "/file" +
            std::to_string(cache_content.header.time_stamp) +
            ".sst";
    // 删除文件
    utils::rmfile(file_name.c_str());
    // 删除缓存区中的信息
    for (auto it = cache_area.begin(); it != cache_area.end(); it++) {
        if (it->level == cache_content.level && it->header.time_stamp == cache_content.header.time_stamp) {
            cache_area.erase(it);
            break;
        }
    }
}