#include "kvstore.h"
#include <string>

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