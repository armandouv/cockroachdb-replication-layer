cmake_minimum_required(VERSION 3.22)
project(crdb_distribution_layer)

set(CMAKE_CXX_STANDARD 20)

add_executable(distribution_layer
        distribution_layer.cpp node.h command.h)
