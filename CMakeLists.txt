cmake_minimum_required(VERSION 3.22 FATAL_ERROR)

# Prefer clang when present.
find_program(CMAKE_C_COMPILER NAMES   $ENV{CC}  clang   gcc PATHS ENV PATH NO_DEFAULT_PATH)
find_program(CMAKE_CXX_COMPILER NAMES $ENV{CXX} clang++ g++ PATHS ENV PATH NO_DEFAULT_PATH)

project(fzcw)

set(ABSL_PROPAGATE_CXX_STD on)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# cmake package manager
include(cmake/CPM.cmake)
add_subdirectory(third-party)

# Enable test discovery, this should come after third-party so we don't execute
# all of _their_ tests as well.
enable_testing()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Werror -gmlt")
# -fsanitize=thread")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS_DEBUG "-Wthread-safety")
endif()

include_directories(inc)
add_subdirectory(lib)
add_subdirectory(src)
add_subdirectory(test)
