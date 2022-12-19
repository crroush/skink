add_executable(zstream-bench zstream_bench.cc)
target_link_libraries(
  zstream-bench
  libskink
  spdlog
  CLI11
  absl::flat_hash_map
)

# For address sanitization: TODO make optional
#target_compile_options(test_flowgraph PRIVATE -fsanitize=address)
#target_link_options(test_flowgraph PRIVATE -fsanitize=address)
