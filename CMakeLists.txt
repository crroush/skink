cmake_minimum_required(VERSION 3.22 FATAL_ERROR)

# Prefer clang when present.
find_program(CMAKE_C_COMPILER NAMES   $ENV{CC}  clang   gcc PATHS ENV PATH NO_DEFAULT_PATH)
find_program(CMAKE_CXX_COMPILER NAMES $ENV{CXX} clang++ g++ PATHS ENV PATH NO_DEFAULT_PATH)

project(skink LANGUAGES C CXX ASM)

set(ABSL_PROPAGATE_CXX_STD on)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# cmake package manager
include(cmake/CPM.cmake)
add_subdirectory(third-party)

# Enable test discovery, this should come after third-party so we don't execute
# all of _their_ tests as well.
enable_testing()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Wpedantic -Werror -gmlt")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS_DEBUG "-g -Wthread-safety")
endif()

include_directories(inc)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(test)
