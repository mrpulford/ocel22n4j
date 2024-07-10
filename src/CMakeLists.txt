cmake_minimum_required(VERSION 3.20)

project(OCEL22N4J LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)


find_package(Boost COMPONENTS program_options REQUIRED )
find_package(SQLite3)

add_subdirectory(3rdparty/curlpp)
add_subdirectory(3rdparty/plog)


add_executable(sqlite2n4j
main.cpp
)

target_link_libraries(sqlite2n4j PRIVATE
    plog::plog
    Boost::program_options
    sqlite3
    curlpp_static
)

target_include_directories(sqlite2n4j PRIVATE
sqlite2n4j
)
