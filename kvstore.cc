#include "kvstore.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{

}

KVStore::~KVStore()
{
    memTable.reset();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    // 计算插入之后 mem_table 的 size 的变化
    uint64_t new_size = memTable.size;
    SkiplistNode* pos;
    if(memTable.search(memTable.head, pos, key))
        new_size += s.size() - pos->value.second.size();
    else
        new_size += 12 + s.size();

    // 若插入之后的 size 超过2MB, 则将 memTable 中的数据转换成 SSTable 保存在 Level 0 层中并清空 memtable
    if(tmp_changer && new_size > 2*1024*1024){
        bool bloom_filter[81920];
        disk.createBloomFilter(memTable, bloom_filter);
        disk.addToCacheArea(memTable, bloom_filter);
        std::string path, name;
        getFilePathName(path, name);
        if(!utils::dirExists(path))
            utils::_mkdir(path.c_str());
        disk.writeToDisk(memTable, bloom_filter, path + "/" + name + ".sst");
        // memTable.reset();
        tmp_changer = false;
    }

    // 执行插入操作
    memTable.insert(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string res;
    if(memTable.search(key, res))
        return res;
    else
        return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    return memTable.del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memTable.reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
 // 默认 key1 < key2
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    // 检索到 key1 对应的最底层节点
    SkiplistNode* start_node;
    if(!memTable.search(memTable.head, start_node, key1)) return;
    while (start_node->below) start_node = start_node->below;
    while (start_node->value.first <= key2 && start_node->value.second != "~DELETED~"){
        list.emplace_back(start_node->value);
        start_node = start_node->next;
    }
}

/**
 * 根据当前的 level 和 file_num 返回应该创建的文件路径
 */
void KVStore::getFilePathName(std::string &path, std::string &name) const
{
    path = "level" + std::to_string(level);
    name = "file" + std::to_string(file_num);
}

/**
 * 当创建一个新的文件后更新 level、file_num、time_stamp
 */
void KVStore::updateContent()
{
    file_num++;
    disk.time_stamp++;
}