// 用于对 LSM KV 存储系统进行性能分析

#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include "kvstore.h"

using namespace std;

int main() {
    /*// 分析不同数据大小(1000 : 10000 : 1000)下 PUT, GET, DELETE 的吞吐量(每秒执行操作个数)
    vector<int> put, get, del;
    uint64_t key, max_key;
    for(int i = 1000; i < 10000; i += 1000) {
        KVStore store("data");
        // 得到 1 s 内执行的操作个数
        int count_put = 0;
        int count_get = 0;
        int count_del = 0;

        key = 0;
        auto start = std::chrono::steady_clock::now();
        auto sec_1 = std::chrono::seconds(1) + start;
        while(std::chrono::steady_clock::now() < sec_1) {
            store.put(++key, std::string(i, 'a'));
            count_put++;
        }
        max_key = key;

        key = 0;
        start = std::chrono::steady_clock::now();
        sec_1 = std::chrono::seconds(1) + start;
        while(std::chrono::steady_clock::now() < sec_1) {
            store.get(++key % max_key);
            count_get++;
        }

        // 在执行删除操作前再插入 5000 个数据
        for(int j = 0; j < 5000; j++) {
            store.put(++max_key, std::string(i, 'a'));
        }

        key = 0;
        start = std::chrono::steady_clock::now();
        sec_1 = std::chrono::seconds(1) + start;
        while(std::chrono::steady_clock::now() < sec_1) {
            store.del(++key);
            count_del++;
        }
        put.emplace_back(count_put);
        get.emplace_back(count_get);
        del.emplace_back(count_del);

        store.reset();
    }
    cout << "PUT: ";
    for(auto i : put) cout << i << ", ";
    cout << endl;
    cout << "     ";
    for(auto i : put) cout << 10000.0 / i << " & ";
    cout << endl;

    cout << "GET: ";
    for(auto i : get) cout << i << ", ";
    cout << endl;
    cout << "     ";
    for(auto i : get) cout << 10000.0 / i << " & ";
    cout << endl;

    cout << "DEL: ";
    for(auto i : del) cout << i << ", ";
    cout << endl;
    cout << "     ";
    for(auto i : del) cout << 10000.0 / i << " & ";
    cout << endl;*/

    // 统计不断插入数据的情况下，PUT的吞吐量
    KVStore store("data");
    vector<int> put_2;
    uint64_t key = 0;
    for(int i = 0; i < 100; i++) {
        int count_put = 0;
        auto start = std::chrono::steady_clock::now();
        auto sec_1 = std::chrono::seconds(1) + start;
        while(std::chrono::steady_clock::now() < sec_1) {
            store.put(++key, std::string(key, 'a'));
            count_put++;
        }
        put_2.emplace_back(count_put);
        cout << i << " ";
    }
    cout << "PUT: ";
    for(auto i : put_2) cout << i << ", ";

    return 0;
}