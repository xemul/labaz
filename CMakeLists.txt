cmake_minimum_required (VERSION 3.27)

project (labaz
  VERSION 0.1.0
  DESCRIPTION "S3- and GCS-compatible object storage"
  LANGUAGES CXX)

# The C++ standard is Seastar's (its CMAKE_CXX_STANDARD cache entry).
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

#
# Seastar, built from the submodule.
#

set (Seastar_APPS OFF CACHE BOOL "" FORCE)
set (Seastar_DEMOS OFF CACHE BOOL "" FORCE)
set (Seastar_DOCS OFF CACHE BOOL "" FORCE)
set (Seastar_TESTING OFF CACHE BOOL "" FORCE)
set (Seastar_INSTALL OFF CACHE BOOL "" FORCE)
set (Seastar_DPDK OFF CACHE BOOL "" FORCE)
# Distribution c-ares 1.34.7/8 is rejected by Seastar for freezing DNS lookups
# (scylladb/seastar#3697). Labaz resolves no names, so accept it by default.
set (Seastar_ASSUME_C_ARES_DNS_FREEZE_FIXED ON CACHE BOOL "")

if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/seastar/CMakeLists.txt")
  message (FATAL_ERROR "The seastar submodule is missing; run `git submodule update --init --recursive`")
endif ()

add_subdirectory (seastar EXCLUDE_FROM_ALL)

#
# The server.
#

find_package (RapidJSON REQUIRED)
find_package (GnuTLS REQUIRED)
find_package (ZLIB REQUIRED)

add_executable (labaz
  src/main.cc
  src/checksum.cc
  src/storage.cc
  src/registry.cc
  src/http_frontend.cc
  src/plywood/plywood_facade.cc
  src/potato/potato_storage.cc)

target_include_directories (labaz
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_options (labaz
  PRIVATE -Wall -Werror)

target_link_libraries (labaz
  PRIVATE
    Seastar::seastar
    RapidJSON
    GnuTLS::GnuTLS
    ZLIB::ZLIB)

#
# Tests.
#

find_package (Python3 REQUIRED COMPONENTS Interpreter)

enable_testing ()

add_test (
  NAME pytest
  COMMAND ${Python3_EXECUTABLE} -m pytest ${CMAKE_CURRENT_SOURCE_DIR}/test
          --labaz=$<TARGET_FILE:labaz>)

add_custom_target (check
  COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
  DEPENDS labaz
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  USES_TERMINAL)
