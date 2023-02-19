# Enable test discovery.
enable_testing()

#add_executable(skinktest random_test.cc zstream_test.cc sizeptr_test.cc)
add_executable(skinktest zstream_test.cc)
target_link_libraries(skinktest libskink absl::flat_hash_map GTest::gtest_main GTest::gmock)

# Wire google tests into CTest.
include(GoogleTest)
gtest_discover_tests(skinktest)
