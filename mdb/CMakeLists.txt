project (minidb)

cmake_minimum_required (VERSION 2.8)

include_directories(.)

add_library(mdb mdb.c)

add_executable(testmdb testmdb.c)
target_link_libraries(testmdb mdb)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wno-unused-function")
