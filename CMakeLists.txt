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

# Default compile options, enable all warnings and minimum line table debug info
# to get good stack traces.
add_compile_options(-Wall -Wextra -Wpedantic -Werror -gmlt)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
   add_compile_options("-g")

   if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
     # Enable thread annotation checking in clang.
     add_compile_options("-Wthread-safety")
   endif()
endif()

# Define configuration options.
set(OPT_NATIVE false CACHE BOOL "Enable -march=native")
if (OPT_NATIVE)
   add_compile_options(-march=native)
endif()

include_directories(inc)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(test)
