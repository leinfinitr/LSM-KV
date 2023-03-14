//
// Created by LEinfinitr on 2023/3/14.
//

#include "SSTable.h"

// 向已经创建好的文件中按照一定的格式存储跳表数据
void SSTable::writeToDisk(SkiplistNode* head, const std::string &file_name)
{
    std::ifstream in(file_name);
    in >> time_stamp
}