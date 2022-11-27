add_executable(fzcw-example fzcw_example.cc)
target_link_libraries(
  fzcw-example
  libfzcw
  spdlog
  CLI11
  absl::flat_hash_map
)

# For address sanitization: TODO make optional
#target_compile_options(test_flowgraph PRIVATE -fsanitize=address)
#target_link_options(test_flowgraph PRIVATE -fsanitize=address)
