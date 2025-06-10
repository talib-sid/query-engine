cmake_minimum_required(VERSION 3.10)
project(query_engine)

# Find Boost package
find_package(Boost REQUIRED COMPONENTS container)

# Include Boost headers
include_directories(${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17)
set(SOURCE_FILES parser.cpp)

add_executable(query_engine ${SOURCE_FILES})

# Link against Boost libraries
target_link_libraries(query_engine ${Boost_LIBRARIES})
