# Enable test discovery.
enable_testing()

add_executable(zstream_test zstream_test.cc)
target_link_libraries(zstream_test libfzcw absl::flat_hash_map GTest::gtest_main GTest::gmock)

# Wire google tests into CTest.
include(GoogleTest)
gtest_discover_tests(zstream_test)
