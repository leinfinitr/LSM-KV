#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "SSTable.h"
#include "utils.h"
#include <string>

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:

public:
    Skiplist memTable;
    SSTable disk;
    int level = 0;  // 用于指示下一次创建 SSTable 时应该在第几层创建
    int file_num = 0;   // 用于指示下一次创建的 SSTable 是该层的第几个文件
    bool tmp_changer = true;

	KVStore(const std::string &dir);
	~KVStore();

	void put(uint64_t key, const std::string &s) override;
	std::string get(uint64_t key) override;
	bool del(uint64_t key) override;
	void reset() override;
	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    void getFilePathName(std::string &path, std::string &name) const;
    void updateContent();
};
