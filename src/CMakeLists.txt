add_executable(zstream-bench zstream_bench.cc)
target_link_libraries(
  zstream-bench
  libskink
  absl::flat_hash_map
  CLI11
  spdlog
)

add_executable(random-bench random_bench.cc)
target_link_libraries(
  random-bench
  absl::numeric
  benchmark_main
)
