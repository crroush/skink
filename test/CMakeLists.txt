# Enable test discovery.
enable_testing()

add_executable(skinktest random_test.cc zstream_test.cc sizeptr_test.cc flexptr_test.cc)
target_link_libraries(skinktest libskink absl::flat_hash_map GTest::gtest_main GTest::gmock)

# Uncomment to get a debug build.
#target_compile_options(skinktest PRIVATE -O0 -ggdb)

# Wire google tests into CTest.
include(GoogleTest)
gtest_discover_tests(skinktest)
