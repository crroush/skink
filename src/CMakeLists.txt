add_executable(zstream-bench zstream_bench.cc)
target_link_libraries(
  zstream-bench
  libskink
  absl::flat_hash_map
  CLI11
  spdlog
)
