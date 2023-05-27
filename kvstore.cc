#include <algorithm>
#include "kvstore.h"

// 保存 dir 目录下的所有文件需要缓存的部分
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir) {
    this->dir = dir;
    disk.dir_name = dir;
    // 得到 dir 目录下的文件夹数量
    std::string path = dir + "/level";
    int level = 0;
    while (utils::dirExists(path + std::to_string(level)))
        level++;
    // 保存所有文件需要缓存的部分
    std::vector<std::string> ret;
    for (int i = 0; i < level; i++) {
        std::string cur_path = dir + "/level" + std::to_string(i);
        utils::scanDir(cur_path, ret);
        for (auto &file: ret) {
            disk.readToCacheArea(cur_path + "/" + file);
        }
        ret.clear();
    }
    // 设置时间戳
    std::vector<uint64_t> res;
    getCacheInfo(res, -1);
    disk.time_stamp = res[2] + 1;
}

KVStore::~KVStore() {
    reset();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    // 计算插入之后 mem_table 的 size 的变化
    uint64_t new_size = memTable.size;
    SkiplistNode *pos;
    if (memTable.search(memTable.head, pos, key))
        new_size += s.size() - pos->value.second.size();
    else
        new_size += 12 + s.size() + 1;

    // 若插入之后的 size 超过2MB, 则将 memTable 中的数据转换成 SSTable 并保存, 而后清空 memtable
    if (new_size > 2 * 1024 * 1024) {
        bool bloom_filter[10240];
        disk.createBloomFilter(memTable, bloom_filter);
        // 遍历 disk.cache_area 中的所有文件, 将其 time_stamp 保存到 level_files 中
        std::vector<std::vector<uint64_t>> level_files; // level_files[i] 表示 level i 中的所有文件的 time_stamp
        for (auto &file: disk.cache_area) {
            int cur_level = file.level;
            uint64_t time_stamp = file.header.time_stamp;
            if (level_files.size() <= cur_level)
                level_files.resize(cur_level + 1);
            level_files[cur_level].push_back(time_stamp);
        }
        // 如果未创建文件或第零层未满，则直接在第零层创建文件
        if (level_files.empty() || level_files[0].size() < file_num_level) {
            std::string path = dir + "/level0";
            if (!utils::dirExists(path))
                utils::_mkdir(path.c_str());
            disk.writeToSSTable(memTable, bloom_filter, 0);
        }
            // 否则在第零层添加完文件后执行 compaction
        else {
            disk.writeToSSTable(memTable, bloom_filter, 0);
            compaction(0);
            // compaction 完成后打印文件信息
//            std::vector<uint64_t> res;
//            getCacheInfo(res, -1);
//            std::cout << "file num: " << res[0] << " min time stamp: " << res[1] << " max time stamp: " << res[2]
//                      << " min key: " << res[3] << " max key: " << res[4] << std::endl;
//            for (auto &file: disk.cache_area)
//                std::cout << "level: " << file.level << " time stamp: " << file.header.time_stamp << " min key: "
//                          << file.header.min_key << " max key: " << file.header.max_key << std::endl;
        }

        memTable.reset();
    }

    // 执行插入操作
    memTable.insert(key, s);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string res;
    if (memTable.search(key, res))
        return res;
    if (res == "~DELETED~")
        return "";
    if (disk.search(key, res))
        return res;
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    std::string res;
    if (memTable.search(key, res)) {
        put(key, "~DELETED~");
        return true;
    }
    if (res == "~DELETED~")
        return false;
    if (disk.search(key, res)) {
        put(key, "~DELETED~");
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    // 遍历 disk.cache_area 中的所有文件, 将其 level 和 time_stamp 保存到 level_files 中
    std::vector<std::vector<uint64_t>> level_files; // level_files[i] 表示 level i 中的所有文件
    for (auto &file: disk.cache_area) {
        int cur_level = file.level;
        uint64_t cur_time_stamp = file.header.time_stamp;
        if (level_files.size() <= cur_level)
            level_files.resize(cur_level + 1);
        level_files[cur_level].push_back(cur_time_stamp);
    }

    // 删除 data 目录下的所有文件
    for (int i = 0; i <= level_files.size(); i++) {
        std::string path = dir;
        std::vector<std::string> ret;
        path += "/level" + std::to_string(i);
        utils::scanDir(path, ret);
        for (auto &file: ret) {
            file = path + "/" + file;
            const char *file_path = file.c_str();
            utils::rmfile(file_path);
        }
        ret.clear();
        utils::rmdir(path.c_str());
    }

    memTable.reset();
    disk.reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
// 默认 key1 < key2
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    for (uint64_t i = key1; i <= key2; i++) {
        std::string res = get(i);
        if (!res.empty())
            list.emplace_back(i, res);
    }
}

// 第 level 层文件超出上限后执行 compaction, compaction 操作不会增加 time_stamp
void KVStore::compaction(int level) {
    // 遍历 disk.cache_area 中的所有文件, 找出 level 层的最小的 time_stamp、最小的 key 和最大的 key
    uint64_t file_num = 0, min_key = UINT64_MAX, max_key = 0, min_time_stamp = UINT64_MAX;
    std::vector<uint64_t> level_info;
    getCacheInfo(level_info, level);
    file_num = level_info[0];
    min_time_stamp = level_info[1];
    min_key = level_info[3];
    max_key = level_info[4];

    // kvs 用于存储所有需要合并文件的键值对, kvs[i][0].key 存储第 i 个文件的 time_stamp
    std::vector<std::vector<std::pair<uint64_t, std::string>>> kvs;
    // 若为第 0 层
    if (level == 0) {
        // 将第 0 层所有 SSTable 文件和第一层中所有 key 范围与最小 key 到最大 key有重叠的 SSTable 文件中的键值对读入 kvs
        for (int i = 0; i < disk.cache_area.size(); i++) {
            auto &file = disk.cache_area[i];
            int cur_level = file.level;
            if (cur_level == 0 || cur_level == 1) {
                uint64_t cur_min_key = file.header.min_key;
                uint64_t cur_max_key = file.header.max_key;
                if (cur_min_key <= max_key && cur_max_key >= min_key) {
                    std::vector<std::pair<uint64_t, std::string>> cur_kvs;
                    // cur_kvs 的第一个元素为 time_stamp
                    cur_kvs.emplace_back(file.header.time_stamp, "");
                    disk.readFromDisk(cur_kvs, file);
                    kvs.push_back(cur_kvs);
                    // 读取成功之后删除文件和对应的缓存
                    disk.deleteFile(file);
                    i--;
                }
            }
        }
    }
        // 若为第 level 层，level > 0
    else {
        // 将第 level 层时间戳最小的文件中的键值对读入 kvs, 直至文件数量满足要求
        uint64_t file_num_to_delete = file_num - (file_num_level << level); // 需要删除的文件数量
        while (file_num_to_delete > 0) {
            for (int i = 0; i < disk.cache_area.size(); i++) {
                auto &file = disk.cache_area[i];
                if (file.level == level && file.header.time_stamp == min_time_stamp) {
                    std::vector<std::pair<uint64_t, std::string>> cur_kvs;
                    cur_kvs.emplace_back(file.header.time_stamp, "");
                    disk.readFromDisk(cur_kvs, file);
                    kvs.push_back(cur_kvs);
                    disk.deleteFile(file);
                    i--;
                    // 更新 min_time_stamp
                    min_time_stamp = UINT64_MAX;
                    for (auto &file2: disk.cache_area) {
                        if (file2.level == level && file2.header.time_stamp < min_time_stamp)
                            min_time_stamp = file2.header.time_stamp;
                    }
                    break;
                }
            }
            file_num_to_delete--;
        }

        // 将第 level + 1 层中所有 key 范围与 level 层所选文件最小 key 到最大 key 有重叠的 SSTable 文件中的键值对读入 kvs
        for (int i = 0; i < disk.cache_area.size(); i++) {
            auto &file = disk.cache_area[i];
            int cur_level = file.level;
            if (cur_level == level + 1) {
                uint64_t cur_min_key = file.header.min_key;
                uint64_t cur_max_key = file.header.max_key;
                if (cur_min_key <= max_key && cur_max_key >= min_key) {
                    std::vector<std::pair<uint64_t, std::string>> cur_kvs2;
                    cur_kvs2.emplace_back(file.header.time_stamp, "");
                    disk.readFromDisk(cur_kvs2, file);
                    kvs.push_back(cur_kvs2);
                    disk.deleteFile(file);
                    i--;
                }
            }
        }
    }
    // 保存所有归并文件的 time_stamp, 用于新文件
    std::vector<uint64_t> time_stamps;
    for (auto &kv: kvs)
        time_stamps.emplace_back(kv[0].first);
    // 对 kvs 执行归并排序，并把结果存入 res_kvs
    std::vector<std::pair<uint64_t, std::string>> res_kvs;
    mergeSort(kvs, res_kvs);
    // 将 res_kvs 中的键值对写入第 level + 1 层的 SSTable 文件
    writeToDisk(res_kvs, level + 1, time_stamps);
    // 判断第 level + 1 层的 SSTable 文件是否超过上限，若超过则执行 compaction
    std::vector<uint64_t> level_next_info;
    getCacheInfo(level_next_info, level + 1);
    if (level_next_info[0] > (file_num_level << (level + 1)))
        compaction(level + 1);
}

// 分析第 level 层的文件的信息
// 若 level = -1，则分析所有文件的信息
// res: 0: 文件数目
//      1: 最小的 time_stamp
//      2: 最大的 time_stamp
//      3: 最小的 key
//      4: 最大的 key
void KVStore::getCacheInfo(std::vector<uint64_t> &res, int level) {
    uint64_t file_num = 0, max_time_stamp = 0, min_key = UINT64_MAX, max_key = 0, min_time_stamp = UINT64_MAX;
    for (auto &file: disk.cache_area) {
        int cur_level = file.level;
        if (level == -1 || cur_level == level) {
            file_num++;
            uint64_t time_stamp = file.header.time_stamp;
            if (time_stamp > max_time_stamp)
                max_time_stamp = time_stamp;
            if (time_stamp < min_time_stamp)
                min_time_stamp = time_stamp;
            uint64_t cur_min_key = file.header.min_key;
            uint64_t cur_max_key = file.header.max_key;
            if (cur_min_key < min_key)
                min_key = cur_min_key;
            if (cur_max_key > max_key)
                max_key = cur_max_key;
        }
    }
    res.push_back(file_num);
    res.push_back(min_time_stamp);
    res.push_back(max_time_stamp);
    res.push_back(min_key);
    res.push_back(max_key);
}

// 归并排序
// kvs: 待排序的键值对, 每一组键值对的第一个元素为该组键值对来源文件的 time_stamp
// ret: 排序后的键值对, 且不包含 time_stamp 和重复的 key
// 每次从 kvs 中选出 time_stamp 最小的两组数据进行归并
void KVStore::mergeSort(std::vector<std::vector<std::pair<uint64_t, std::string>>> &kvs,
                        std::vector<std::pair<uint64_t, std::string>> &ret) {
    uint64_t size = kvs.size();
    if (size == 1) {
        // 删除 ksv[0] 中的 time_stamp
        kvs[0].erase(kvs[0].begin());
        ret = kvs[0];
        return;
    }
    // 得到 kvs 中 time_stamp 最小的两个值, min_time_stamp1 < min_time_stamp2
    uint64_t min_time_stamp1, min_time_stamp2;
    int min_time_stamp1_index, min_time_stamp2_index;
    if (kvs[0][0].first < kvs[1][0].first) {
        min_time_stamp1 = kvs[0][0].first;
        min_time_stamp1_index = 0;
        min_time_stamp2 = kvs[1][0].first;
        min_time_stamp2_index = 1;
    } else {
        min_time_stamp1 = kvs[1][0].first;
        min_time_stamp1_index = 1;
        min_time_stamp2 = kvs[0][0].first;
        min_time_stamp2_index = 0;
    }
    for (int i = 2; i < size; i++) {
        uint64_t cur_time_stamp = kvs[i][0].first;
        if (cur_time_stamp < min_time_stamp1) {
            min_time_stamp2 = min_time_stamp1;
            min_time_stamp2_index = min_time_stamp1_index;
            min_time_stamp1 = cur_time_stamp;
            min_time_stamp1_index = i;
        } else if (cur_time_stamp < min_time_stamp2) {
            min_time_stamp2 = cur_time_stamp;
            min_time_stamp2_index = i;
        }
    }
    // 设置 ret 的 time_stamp
    std::vector<std::pair<uint64_t, std::string>> res;
    kvs[min_time_stamp1_index].erase(kvs[min_time_stamp1_index].begin());
    kvs[min_time_stamp2_index].erase(kvs[min_time_stamp2_index].begin());
    res.emplace_back(min_time_stamp2, "");
    // 对 kvs[min_time_stamp1_index] 和 kvs[min_time_stamp1_index] 执行归并排序，并将结果存入 res
    while (!kvs[min_time_stamp1_index].empty()) {
        // 若 kvs[min_time_stamp2_index] 已空，则将 kvs[min_time_stamp1_index] 中剩余的键值对全部存入 res
        if (kvs[min_time_stamp2_index].empty()) {
            res.insert(res.end(), kvs[min_time_stamp1_index].begin(), kvs[min_time_stamp1_index].end());
            kvs[min_time_stamp1_index].clear();
            break;
        }
        // 若 kvs[min_time_stamp1_index] 和 kvs[min_time_stamp2_index] 的第一个键值对的 key 相同，则将 kvs[min_time_stamp2_index] 的第一个键值对存入 res
        if (kvs[min_time_stamp1_index][0].first < kvs[min_time_stamp2_index][0].first) {
            res.emplace_back(kvs[min_time_stamp1_index][0]);
            kvs[min_time_stamp1_index].erase(kvs[min_time_stamp1_index].begin());
        } else if (kvs[min_time_stamp1_index][0].first == kvs[min_time_stamp2_index][0].first) {
            res.emplace_back(kvs[min_time_stamp2_index][0]);
            kvs[min_time_stamp1_index].erase(kvs[min_time_stamp1_index].begin());
            kvs[min_time_stamp2_index].erase(kvs[min_time_stamp2_index].begin());
        } else {
            res.emplace_back(kvs[min_time_stamp2_index][0]);
            kvs[min_time_stamp2_index].erase(kvs[min_time_stamp2_index].begin());
        }
    }
    while (!kvs[min_time_stamp2_index].empty()) {
        res.insert(res.end(), kvs[min_time_stamp2_index].begin(), kvs[min_time_stamp2_index].end());
        kvs[min_time_stamp2_index].clear();
    }
    // 删除 kvs[min_time_stamp1_index] 和 kvs[min_time_stamp2_index]，将 res 加入 kvs
    if (min_time_stamp1_index > min_time_stamp2_index) {
        kvs.erase(kvs.begin() + min_time_stamp2_index);
        kvs.erase(kvs.begin() + min_time_stamp1_index - 1);
    } else {
        kvs.erase(kvs.begin() + min_time_stamp1_index);
        kvs.erase(kvs.begin() + min_time_stamp2_index - 1);
    }
    kvs.push_back(res);
    // 递归调用 mergeSort
    mergeSort(kvs, ret);
}

// 在完成 compaction 之后, 将 kvs 中的键值对写入第 level 层的 SSTable 文件
// 这些文件的 time_stamp 从合并文件的 time_stamps 中依次选取, LSM Tree 的性质保证了这些 time_stamp 的合理性
// 若 level 层文件目录不存在，将自动创建
void KVStore::writeToDisk(std::vector<std::pair<uint64_t, std::string>> &kvs, int level,
                          std::vector<uint64_t> &time_stamps) {
    // 创建 level 层文件目录
    std::string path = dir + "/level" + std::to_string(level);
    if (!utils::dirExists(path))
        utils::_mkdir(path.c_str());
    // 创建一个只有一层的跳表用于存储一个文件的所有键值对
    Skiplist skip_list;
    auto *node = skip_list.head; // node 指向最后一个节点
    bool new_file_flag = true;
    for (auto &kv: kvs) {
        // 计算插入之后 skip_list 的 size 的变化
        uint64_t new_size = skip_list.size;
        new_size += 12 + kv.second.size() + 1;

        // 若插入之后的 size 超过2MB, 则将 skip_list 中的数据转换成 SSTable 并保存, 而后清空 skip_list
        if (new_size > 2 * 1024 * 1024) {
            // 设置最大 key
            skip_list.max_key = node->value.first;
            bool bloom_filter[10240];
            disk.createBloomFilter(skip_list, bloom_filter);
            // 使用 time_stamps 中最小的 time_stamp 创建一个新的 SSTable 文件
            uint64_t time_stamp = time_stamps[0];
            time_stamps.erase(time_stamps.begin());
            disk.writeToSSTable(skip_list, bloom_filter, level, time_stamp);

            // 清空 skip_list
            skip_list.reset();
            node = skip_list.head;
            new_file_flag = true;
            skip_list.size += 12 + kv.second.size() + 1;
        } else
            skip_list.size = new_size;
        // 如果是跳表的第一个节点，则将 min_key 设置为当前键值对的 key
        if (new_file_flag) {
            new_file_flag = false;
            skip_list.min_key = kv.first;
        }
        // 如果 key 为零，直接将 value 写入
        if (kv.first == 0) {
            node->value.second = kv.second;
            skip_list.count++;
        }
            // 否则创建新节点并使其互相连接
        else {
            node->next = new SkiplistNode(kv.first, kv.second);
            node->next->pre = node;
            node = node->next;
            skip_list.count++;
        }
    }
    // 将剩余的键值对写入 SSTable
    if (skip_list.count > 0) {
        skip_list.max_key = node->value.first;
        bool bloom_filter[10240];
        disk.createBloomFilter(skip_list, bloom_filter);
        uint64_t time_stamp = time_stamps[0];
        time_stamps.erase(time_stamps.begin());
        disk.writeToSSTable(skip_list, bloom_filter, level, time_stamp);
    }
    // 清空 skip_list
    skip_list.reset();
}

// 辅助分析函数，用于分析某个键值对的位置
void KVStore::analysis(uint64_t key) {
    SkiplistNode *pos;
    memTable.search(memTable.head, pos, key);
    std::cout << "mem search : " << pos->value.first << " " << pos->value.second << std::endl;

    uint64_t search_key = key;
    std::string search_value;
    std::vector<std::pair<std::string, uint64_t>> search_res;
    // 遍历所有缓存区
    for (auto &i: disk.cache_area) {
        // 如果查找的键落入该缓存区的键值范围内
        if (search_key >= i.header.min_key && search_key <= i.header.max_key) {
            // 如果 Bloom Filter 判断该键值对可能在缓存区中
            if (disk.bloomFilter(search_key, i.bloom_filter)) {
                // 从索引区中查找该键值对
                uint32_t offset = disk.binarySearch(i.index_area, search_key);
                if (offset != 0) {
                    // 从磁盘中读取数据
                    std::ifstream in;
                    in.open(dir + "/level" + std::to_string(i.level) + "/file" +
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
    // 输出 search_res
    std::cout << "disk search : ";
    for (auto &i: search_res)
        std::cout << i.first << " " << i.second << " ";
    std::cout << std::endl;
}

// 输出文件夹结构
void KVStore::printFileLevel() {
    std::vector<uint64_t> res;
    for (const auto &i: disk.cache_area)
        std::cout << "level" << i.level << " file" << i.header.time_stamp << std::endl;
}