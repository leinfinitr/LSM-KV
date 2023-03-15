//
// Created by LEinfinitr on 2023/2/27.
//

#include "skiplist.h"

#include <utility>

// 从跳表中查找数值
// search_value 为要查找的数值， search_value 为查找结果
// 若跳表中存在该键值对，且该键值对不为删除标记则返回 ture
// 否则返回 false
bool Skiplist::search(uint64_t search_key, std::string& search_value)
{
    SkiplistNode* res;  // 存储本次查找到的节点的位置
    // 从跳表的头节点开始查找
    bool search_res = search(head, res, search_key);

    if(search_res && res->value.second == "~DELETED~") return false;

    search_value = res->value.second;
    return search_res;
}

// 从某一个结点开始，向右向下查找数值
// 若查找成功，则返回 true，并且 pos 指向被查找的元素
// 若查找失败，则返回 false，并且 pos 指向最后一个被查找的元素
bool Skiplist::search(SkiplistNode *search_head, SkiplistNode *&pos, uint64_t search_key)
{
    // 若查找起始节点为空，则直接返回 false
    if(search_head == nullptr) return false;
    // 若起始节点的后方节点不为空，且要查找的值（下称目标值）大于后方节点的值，则一直向该层后方查找
    while (search_head->next != nullptr && search_head->next->value.first <= search_key){
        search_head = search_head->next;
    }

    // 若当前节点的值与目标值相同，则返回 true
    if(search_head->value.first == search_key){
        pos = search_head;
        return true;
    }
    // 若当前节点下方节点为空，则代表已经检索到最后一个节点，查找失败
    if(search_head->below == nullptr){
        pos = search_head;
        return false;
    }
    // 否则进入下一层查找
    return search(search_head->below, pos ,search_key);
}

// 插入算法
// 如果存在键 K 的记录，则进行覆盖操作
void Skiplist::insert(uint64_t insert_key, const std::string& insert_value)
{
    SkiplistNode* pos;  // 保存查找算法的最终查找节点
    // 若查找成功，则进行覆盖操作
    if(search(head, pos, insert_key)) {
        // 更新 size
        size += insert_value.size() - pos->value.second.size();
        // 更改所有层的数值
        while (pos) {
            pos->value.second = insert_value;
            pos = pos->below;
        }
    }
    else{
        // 键值对数量 +1, 并调整 max_key 和 min_key
        count ++;
        if(insert_key < min_key) min_key = insert_key;
        if(insert_key > max_key) max_key = insert_key;
        // 更新 size
        size += 12 + insert_value.size();

        // 首先在最底层插入节点
        // 经过查找算法，pos 指向被插入位置的前一个结点
        SkiplistNode* tmp = pos->next;  // 保存插入位置后一个节点的地址
        auto* new_node = new SkiplistNode(insert_key, insert_value);    // 创建节点
        // 节点互相链接
        pos->next = new_node;
        new_node->next = tmp;
        new_node->pre = pos;
        if(tmp != nullptr) tmp->pre = new_node;
        // 让 tmp 指向当前节点
        tmp = new_node;
        // 向上成塔
        while (rand() % 100 <= 50){    // 按照 1/2 概率生长
            // 找到当前层上一个通向上一层的节点
            while (tmp->pre != nullptr && tmp->above == nullptr)
                tmp = tmp->pre;
            // 如果达到整个跳表的头节点
            if(tmp->pre == nullptr && tmp->above == nullptr){
                // 新建一层，并让 head 指向新的头节点
                tmp->above = new SkiplistNode();
                head = tmp->above;
                // 更新新的头节点的值
                head->value.second = tmp->value.second;
                // 让新的头节点的 below 指向旧的头节点
                head->below = tmp;
                // 创建顶层的增长节点
                auto* above_node = new SkiplistNode(insert_key, insert_value);
                // 节点互相链接
                head->next = above_node;
                above_node->pre = head;
                above_node->below = new_node;
                new_node->above = above_node;
                // 层数+1
                level++;
                break;
            } else {    // 未达到整个跳表的头节点，即 tmp->above != nullptr
                // 创建增长节点
                auto* above_node = new SkiplistNode(insert_key, insert_value);
                // 增长节点与相邻节点的互相链接
                SkiplistNode* tmp2 = tmp->above->next;
                tmp->above->next = above_node;
                above_node->pre = tmp->above;
                above_node->next = tmp2;
                above_node->below = new_node;
                new_node->above = above_node;
                if(tmp2 != nullptr) tmp2->pre = above_node;

                // 更改 new_node 和 tmp 指向的位置，进行下一次循环
                tmp = new_node = above_node;
            }
        }
    }
}

//    删除操作
//    如果未查找到记录，则不需要进行删除操作，返回 false
//    若搜索到记录但该记录为删除标记，则返回 false
//    若搜索到记录，则在MemTable中再插入一条记录，表示键 K 被删除了，并返回 true。
//        此特殊的记录为 “删除标记”，其键为 K ，值为特殊字符串 “~DELETED~”
bool Skiplist::del(uint64_t search_key)
{
    SkiplistNode* res;
    if(search(head, res, search_key)) {
        if(res->value.second == "~DELETED~") return false;
        insert(search_key, "~DELETED~");
        return true;
    }
    else
        return false;
}

//    清空 Skiplist 并释放动态内存
void Skiplist::reset()
{
    SkiplistNode* p = head;
    // 逐层删除
    while (p != nullptr){
        SkiplistNode* tmp = p->below;
        // 删除 p 层
        while (p != nullptr) {
            SkiplistNode* p2 = p->next;
            delete p;
            p = p2;
        }
        p = tmp;
    }

    // 让 head 指向新的初始节点
    level = 1;
    count = 0;
    max_key = 0;
    min_key = UINT64_MAX;
    head = new SkiplistNode();
}

// 打印跳表
void Skiplist::print() const
{
    SkiplistNode* p = head;
    while (p != nullptr){
        SkiplistNode* p2 = p->next;
        while (p2 != nullptr) {
            std::cout << p2->value.first << ":" << p2->value.second << " ";
            p2 = p2->next;
        }
        std::cout << "\n";
        p = p->below;
    }
}