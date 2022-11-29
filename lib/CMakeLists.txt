add_library(libfzcw STATIC zstream.cc)
target_include_directories(libfzcw PUBLIC ${PROJECT_SOURCE_DIR}/inc)

# Stop cmake from prepending lib to the name
set_target_properties(libfzcw PROPERTIES PREFIX "")
target_link_libraries(libfzcw PUBLIC
  absl::synchronization
  spdlog::spdlog
)
