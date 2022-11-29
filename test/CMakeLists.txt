# Enable test discovery.
enable_testing()

add_executable(fzcwtest zstream_test.cc sizeptr_test.cc)
target_link_libraries(fzcwtest libfzcw absl::flat_hash_map GTest::gtest_main GTest::gmock)

# Wire google tests into CTest.
include(GoogleTest)
gtest_discover_tests(fzcwtest)
