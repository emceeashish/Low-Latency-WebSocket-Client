cmake_minimum_required(VERSION 3.18)
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()
project(low_latency_ws_client LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Boost REQUIRED COMPONENTS system)
find_package(Threads REQUIRED)

add_executable(ws_client src/main.cpp)
target_include_directories(ws_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(ws_client PRIVATE Boost::system Threads::Threads)

if(WIN32)
    target_link_libraries(ws_client PRIVATE ws2_32)
endif()

if(MSVC)
    target_compile_options(ws_client PRIVATE /O2 /W4)
else()
    target_compile_options(ws_client PRIVATE -O3 -Wall -Wextra)
endif()
