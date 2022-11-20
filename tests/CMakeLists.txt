add_executable(test-fzcw test_fzcw.cc)
target_link_libraries(test-fzcw libfzcw gtest gtest_main )

add_test(test-fzcw test-fzcw )

