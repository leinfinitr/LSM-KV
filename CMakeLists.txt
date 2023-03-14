cmake_minimum_required(VERSION 3.21)
project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSMKV
#        main.cpp
        correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
#        persistence.cc
        test.h
        utils.h
        skiplist.cpp
        skiplist.h SSTable.cpp SSTable.h)