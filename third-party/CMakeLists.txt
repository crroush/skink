# Dependency install path
set (DEPS_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/opt)

# CPM packages
CPMAddPackage("gh:gabime/spdlog#v1.11.0")
CPMAddPackage("gh:CLIUtils/CLI11#v2.3.1")
CPMAddPackage(
  NAME googletest
  GITHUB_REPOSITORY google/googletest
  GIT_TAG release-1.12.1
  VERSION 1.12.1
  OPTIONS "INSTALL_GTEST_OFF"
)

CPMAddPackage(
  NAME abseil
  GITHUB_REPOSITORY abseil/abseil-cpp
  GIT_TAG 20220623.1
  VERSION 20220623
  OPTIONS "ABSL_PROPAGATE_CXX_STD ON"
)

CPMAddPackage(
  NAME googlebench
  GITHUB_REPOSITORY google/benchmark
  GIT_TAG v1.7.1
  VERSION v1.7.1
  OPTIONS "BENCHMARK_ENABLE_TESTING OFF"
)

# Add the install path to the search path for pkgconfig
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_BINARY_DIR}/opt/usr)
include(FetchContent)
find_package(PkgConfig REQUIRED)
