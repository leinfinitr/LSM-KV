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
class SkiplistNode{
public:
    std::pair<uint64_t, std::string> value;
    SkiplistNode* pre = nullptr;
    SkiplistNode* next = nullptr;
    SkiplistNode* below = nullptr;
    SkiplistNode* above = nullptr;

    explicit SkiplistNode(uint64_t k = 0, std::string v = "", SkiplistNode* p = nullptr, SkiplistNode* n = nullptr,
                 SkiplistNode* b = nullptr, SkiplistNode* a = nullptr){
        value.first = k;
        value.second = std::move(v);
        pre = p;
        next = n;
        below = b;
        above = a;
    }
};

class Skiplist{
public:
    int level;  // 跳表层数
    int count;  // 键值对数量
    uint64_t min_key, max_key;  // 键最大值和最小值
    SkiplistNode* head;

    Skiplist(){
        level = 1;
        count = 0;
        min_key = max_key = 0;
        head = new SkiplistNode();
    }

    bool search(SkiplistNode* search_head, SkiplistNode* &pos, uint64_t search_key);

    //    查找函数
    //    如果查找到记录返回 true 并将结果存储在 search_value 中
    //    如果未查找到记录则返回 false
    bool search(uint64_t search_key, std::string& insert_value);
    //    插入函数
    void insert(uint64_t insert_key, const std::string& insert_value);
    //    删除操作
    //    如果未查找到记录，则不需要进行删除操作，返回 false
    //    若搜索到记录，则在MemTable中再插入一条记录，表示键 K 被删除了，并返回 true。
    //        此特殊的记录为 “删除标记”，其键为 K ，值为特殊字符串 “~DELETED~”
    bool del(uint64_t search_key);
    //    清空 Skiplist 并释放动态内存
    void reset();

    void print() const; // 打印整个跳表
};

#endif //HW2_SKIPLIST_H
