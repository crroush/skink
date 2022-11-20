cmake_minimum_required(VERSION 3.22 FATAL_ERROR)
project(fzcw)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# cmake package manager
include(cmake/CPM.cmake)

# enable test discovery
enable_testing()

add_subdirectory(third-party)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(tests)
