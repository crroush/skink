add_library(libfzcw STATIC zstream.cc memcpy.S)
target_include_directories(libfzcw PUBLIC ${PROJECT_SOURCE_DIR}/inc)

# Assemble memcpy.S with g++ so preprocessor defines work.
set_property(SOURCE memcpy.S PROPERTY LANGUAGE CXX)
set_property(SOURCE memcpy.S
    APPEND PROPERTY COMPILE_OPTIONS "-x" "assembler-with-cpp")

# Stop cmake from prepending lib to the name
set_target_properties(libfzcw PROPERTIES PREFIX "")
target_link_libraries(libfzcw PUBLIC
  absl::synchronization
  spdlog::spdlog
)
