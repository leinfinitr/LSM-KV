//
// Created by LEinfinitr on 2023/2/27.
//

#include "skiplist.h"

// 从跳表中查找记录
// 如果查找到记录返回 true 并将结果存储在 search_value 中
// 如果未查找到记录或查找到删除标记，则返回 false
bool Skiplist::search(uint64_t search_key, std::string &search_value) {
    SkiplistNode *res;  // 存储本次查找到的节点的位置
    // 若跳表为空, 或者要查找的键为 0 且跳表的头节点的值为空，则直接返回 false
    if (head == nullptr || (search_key == 0 && head->value.second.empty())) return false;
    // 从跳表的头节点开始查找
    bool search_res = search(head, res, search_key);
    // 若查找到删除标记，则返回 false
    if (search_res && res->value.second == "~DELETED~") {
        search_value = "~DELETED~";
        return false;
    }
    // 若查找到不为删除标记的值
    if (search_res)
        search_value = res->value.second;
    else
        search_value = "";
    return search_res;
}

// 从某一个结点开始，向右向下查找数值
// 若查找成功，则返回 true，并且 pos 指向被查找的元素
// 若查找失败，则返回 false，并且 pos 指向最后一个被查找的元素
bool Skiplist::search(SkiplistNode *search_head, SkiplistNode *&pos, uint64_t search_key) {
    // 若查找起始节点为空，则直接返回 false
    if (search_head == nullptr) return false;
    // 若起始节点的后方节点不为空，且要查找的值（下称目标值）大于后方节点的值，则一直向该层后方查找
    while (search_head->next != nullptr && search_head->next->value.first <= search_key) {
        search_head = search_head->next;
    }

    // 若当前节点的键值与目标键值相同，则返回 true
    if (search_head->value.first == search_key) {
        pos = search_head;
        return true;
    }
    // 若当前节点下方节点为空，则代表已经检索到最后一个节点，查找失败
    if (search_head->below == nullptr) {
        pos = search_head;
        return false;
    }
    // 否则进入下一层查找
    return search(search_head->below, pos, search_key);
}

// 向跳表中插入记录
// 如果存在键 K 的记录，则进行覆盖操作
void Skiplist::insert(uint64_t insert_key, const std::string &insert_value) {
    SkiplistNode *pos;  // 保存查找算法的最终查找节点
    // 若查找成功，则进行覆盖操作
    if (search(head, pos, insert_key)) {
        // 由于当 key = 0 时一定会查找成功，故如果之前 key = 0 且不存在数据，需要更新 min_key 和 count
        if (insert_key == 0 && head->value.second.empty()) {
            min_key = 0;
            count++;
        }
        // 更新 size
        size += insert_value.size() - pos->value.second.size();
        // 更改所有层的数值
        // 由于查找到的节点一定是最顶层的节点，故只需要向下遍历即可
        while (pos) {
            pos->value.second = insert_value;
            pos = pos->below;
        }
    }
        // 若查找不成功，则进行插入操作
    else {
        // 键值对数量 +1, 并调整 max_key 和 min_key
        count++;
        if (insert_key < min_key) min_key = insert_key;
        if (insert_key > max_key) max_key = insert_key;
        // 更新 size
        size += 12 + insert_value.size() + 1;

        // 首先在最底层插入节点
        // 经过查找算法，pos 指向被插入位置的前一个结点
        SkiplistNode *tmp = pos->next;  // 保存插入位置后一个节点的地址
        auto *new_node = new SkiplistNode(insert_key, insert_value);    // 创建节点
        // 节点互相链接
        pos->next = new_node;
        new_node->next = tmp;
        new_node->pre = pos;
        if (tmp != nullptr) tmp->pre = new_node;
        // 让 tmp 指向当前节点
        tmp = new_node;
        // 向上成塔
        while (rand() % 100 <= 50) {    // 按照 1/2 概率生长
            // 找到当前层上一个通向上一层的节点
            while (tmp->pre != nullptr && tmp->above == nullptr)
                tmp = tmp->pre;
            // 如果达到整个跳表的头节点
            if (tmp->pre == nullptr && tmp->above == nullptr) {
                // 新建一层，并让 head 指向新的头节点
                tmp->above = new SkiplistNode();
                head = tmp->above;
                // 更新新的头节点的值
                head->value.second = tmp->value.second;
                // 让新的头节点的 below 指向旧的头节点
                head->below = tmp;
                // 创建顶层的增长节点
                auto *above_node = new SkiplistNode(insert_key, insert_value);
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
                auto *above_node = new SkiplistNode(insert_key, insert_value);
                // 增长节点与相邻节点的互相链接
                SkiplistNode *tmp2 = tmp->above->next;
                tmp->above->next = above_node;
                above_node->pre = tmp->above;
                above_node->next = tmp2;
                above_node->below = new_node;
                new_node->above = above_node;
                if (tmp2 != nullptr) tmp2->pre = above_node;

                // 更改 new_node 和 tmp 指向的位置，进行下一次循环
                tmp = new_node = above_node;
            }
        }
    }
}

//   清空 Skiplist 并释放动态内存
//   重置 Skiplist 的所有成员变量
void Skiplist::reset() {
    SkiplistNode *p = head;
    // 逐层删除
    while (p != nullptr) {
        SkiplistNode *tmp = p->below;
        // 删除 p 层
        while (p != nullptr) {
            SkiplistNode *p2 = p->next;
            delete p;
            p = p2;
        }
        p = tmp;
    }

    // 重置 Skiplist 的成员变量
    level = 1;
    count = 0;
    max_key = 0;
    min_key = UINT64_MAX;
    size = 10272;
    head = new SkiplistNode();
}

// 打印跳表
void Skiplist::print() const {
    SkiplistNode *p = head;
    while (p != nullptr) {
        SkiplistNode *p2 = p->next;
        while (p2 != nullptr) {
            std::cout << p2->value.first << ":" << p2->value.second << " ";
            p2 = p2->next;
        }
        std::cout << "\n";
        p = p->below;
    }
}

// 打印跳表所有的 key
void Skiplist::printKey() const {
    SkiplistNode *p = head;
    while (p->below != nullptr) {
        p = p->below;
    }
    if (p->value.second.empty()) p = p->next;
    while (p != nullptr) {
        std::cout << p->value.first << " ";
        p = p->next;
    }
}