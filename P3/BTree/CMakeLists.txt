cmake_minimum_required(VERSION 3.14)
project(b_tree)

set(CMAKE_CXX_STANDARD 14)

include_directories(Btree/src)
include_directories(Btree/src/exceptions)

add_executable(b_tree
        Btree/src/exceptions/bad_buffer_exception.cpp
        Btree/src/exceptions/bad_buffer_exception.h
        Btree/src/exceptions/bad_index_info_exception.cpp
        Btree/src/exceptions/bad_index_info_exception.h
        Btree/src/exceptions/bad_opcodes_exception.cpp
        Btree/src/exceptions/bad_opcodes_exception.h
        Btree/src/exceptions/bad_scan_param_exception.cpp
        Btree/src/exceptions/bad_scan_param_exception.h
        Btree/src/exceptions/bad_scanrange_exception.cpp
        Btree/src/exceptions/bad_scanrange_exception.h
        Btree/src/exceptions/badgerdb_exception.cpp
        Btree/src/exceptions/badgerdb_exception.h
        Btree/src/exceptions/buffer_exceeded_exception.cpp
        Btree/src/exceptions/buffer_exceeded_exception.h
        Btree/src/exceptions/end_of_file_exception.cpp
        Btree/src/exceptions/end_of_file_exception.h
        Btree/src/exceptions/file_exists_exception.cpp
        Btree/src/exceptions/file_exists_exception.h
        Btree/src/exceptions/file_not_found_exception.cpp
        Btree/src/exceptions/file_not_found_exception.h
        Btree/src/exceptions/file_open_exception.cpp
        Btree/src/exceptions/file_open_exception.h
        Btree/src/exceptions/hash_already_present_exception.cpp
        Btree/src/exceptions/hash_already_present_exception.h
        Btree/src/exceptions/hash_not_found_exception.cpp
        Btree/src/exceptions/hash_not_found_exception.h
        Btree/src/exceptions/hash_table_exception.cpp
        Btree/src/exceptions/hash_table_exception.h
        Btree/src/exceptions/index_scan_completed_exception.cpp
        Btree/src/exceptions/index_scan_completed_exception.h
        Btree/src/exceptions/insufficient_space_exception.cpp
        Btree/src/exceptions/insufficient_space_exception.h
        Btree/src/exceptions/invalid_page_exception.cpp
        Btree/src/exceptions/invalid_page_exception.h
        Btree/src/exceptions/invalid_record_exception.cpp
        Btree/src/exceptions/invalid_record_exception.h
        Btree/src/exceptions/invalid_slot_exception.cpp
        Btree/src/exceptions/invalid_slot_exception.h
        Btree/src/exceptions/no_such_key_found_exception.cpp
        Btree/src/exceptions/no_such_key_found_exception.h
        Btree/src/exceptions/page_not_pinned_exception.cpp
        Btree/src/exceptions/page_not_pinned_exception.h
        Btree/src/exceptions/page_pinned_exception.cpp
        Btree/src/exceptions/page_pinned_exception.h
        Btree/src/exceptions/scan_not_initialized_exception.cpp
        Btree/src/exceptions/scan_not_initialized_exception.h
        Btree/src/exceptions/slot_in_use_exception.cpp
        Btree/src/exceptions/slot_in_use_exception.h
        Btree/src/btree.cpp
        Btree/src/btree.h
        Btree/src/buffer.cpp
        Btree/src/buffer.h
        Btree/src/bufHashTbl.cpp
        Btree/src/bufHashTbl.h
        Btree/src/file.cpp
        Btree/src/file.h
        Btree/src/file_iterator.h
        Btree/src/filescan.cpp
        Btree/src/filescan.h
        Btree/src/main.cpp
        Btree/src/main.hpp
        Btree/src/page.cpp
        Btree/src/page.h
        Btree/src/page_iterator.h
        Btree/src/types.h)
