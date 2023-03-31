cmake_minimum_required(VERSION 3.23)
project(stats)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS -O2)

find_package(MPI REQUIRED)

include_directories(D:/C++SDK/boost_1_81_0)
include_directories(D:/C++SDK/rapidjson/include)

link_directories(D:/C++SDK/boost_1_81_0/stage/lib)

add_executable(stats main.cpp)

target_link_libraries(stats PUBLIC MPI::MPI_CXX boost_program_options-mgw8-mt-d-x64-1_81 boost_program_options-mgw8-mt-x64-1_81
        boost_serialization-mgw8-mt-d-x64-1_81 boost_serialization-mgw8-mt-x64-1_81)
