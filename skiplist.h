//
// Created by LEinfinitr on 2023/2/27.
//

#ifndef HW2_SKIPLIST_H
#define HW2_SKIPLIST_H

#include <random>
#include <iostream>
#include <ctime>
#include <cmath>
#include <utility>

// 跳表中的所有值均为正整数
class SkiplistNode {
public:
    std::pair<uint64_t, std::string> value;
    SkiplistNode *pre = nullptr;
    SkiplistNode *next = nullptr;
    SkiplistNode *below = nullptr;
    SkiplistNode *above = nullptr;

    explicit SkiplistNode(uint64_t k = 0, std::string v = "", SkiplistNode *p = nullptr, SkiplistNode *n = nullptr,
                          SkiplistNode *b = nullptr, SkiplistNode *a = nullptr) {
        value.first = k;
        value.second = std::move(v);
        pre = p;
        next = n;
        below = b;
        above = a;
    }
};

class Skiplist {
public:
    int level;  // 跳表层数
    uint64_t count;  // 键值对数量
    uint64_t min_key, max_key;  // 键最大值和最小值
    uint64_t size;  // 跳表转化为 SSTable 后的大小
    SkiplistNode *head;

    Skiplist() {
        level = 1;
        count = 0;
        max_key = 0;
        min_key = UINT64_MAX;
        size = 10272;
        head = new SkiplistNode();
    }

    bool search(uint64_t search_key, std::string &insert_value);

    void insert(uint64_t insert_key, const std::string &insert_value);

    void reset();

    void print() const;

    void printKey() const;

    bool search(SkiplistNode *search_head, SkiplistNode *&pos, uint64_t search_key);
};

#endif //HW2_SKIPLIST_H
