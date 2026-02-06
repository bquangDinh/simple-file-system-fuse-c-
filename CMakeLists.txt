cmake_minimum_required(VERSION 3.20)

project(myfs LANGUAGES CXX)

# C++ settings
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Sources
add_library(myfs_lib
    src/block.cpp
    src/data.cpp
    src/dirent.cpp
    src/inode.cpp
    src/superblock.cpp
    src/utilities.cpp
)

target_include_directories(myfs_lib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_executable(myfs
    src/main.cpp
)

target_link_libraries(myfs
    PRIVATE myfs_lib
)

# Threads
find_package(Threads REQUIRED)
target_link_libraries(myfs PRIVATE Threads::Threads)

# Math
target_link_libraries(myfs PRIVATE m)

# FUSE
find_package(PkgConfig REQUIRED)

pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3)
target_link_libraries(myfs PRIVATE PkgConfig::FUSE3)

# Sanitizer
option(ENABLE_ASAN "Enable AddressSanitizer" ON)

if (ENABLE_SAN)
    target_compile_options(myfs PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(myfs PRIVATE -fsanitize=address,undefined)
endif()

target_compile_options(myfs PRIVATE -Wall -Wextra -Wpedantic)