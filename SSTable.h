//
// Created by LEinfinitr on 2023/3/14.
//

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H

#include "utils.h"
#include "skiplist.h"
#include <iostream>
#include <fstream>

class SSTable{
public:
    int time_stamp;

    void writeToDisk(SkiplistNode* head, const std::string &file_name);
};

#endif //LSMKV_SSTABLE_H
