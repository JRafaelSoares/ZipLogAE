cmake_minimum_required(VERSION 3.16)
project(ziplog LANGUAGES C CXX)

option(ZIPLOG_BUILD_TESTS "Build ZipLog tests" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

# Targets below enable per-target INTERPROCEDURAL_OPTIMIZATION (LTO) in Release builds.
# clang emits LLVM bitcode objects for LTO, which plain binutils ar/ranlib cannot index
# ("archive has no index"), so point CMake at the matching llvm-ar/llvm-ranlib instead.
find_program(LLVM_AR_PATH NAMES llvm-ar llvm-ar-10)
find_program(LLVM_RANLIB_PATH NAMES llvm-ranlib llvm-ranlib-10)
if(LLVM_AR_PATH AND LLVM_RANLIB_PATH)
    set(CMAKE_CXX_COMPILER_AR ${LLVM_AR_PATH})
    set(CMAKE_CXX_COMPILER_RANLIB ${LLVM_RANLIB_PATH})
    set(CMAKE_C_COMPILER_AR ${LLVM_AR_PATH})
    set(CMAKE_C_COMPILER_RANLIB ${LLVM_RANLIB_PATH})
endif()

# Optimization flags - DO NOT use -flto in CMAKE_CXX_FLAGS globally
# as it conflicts with C libraries built without LTO
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -mavx")
# LTO will be enabled per-target for C++ targets only

if(NOT ERPC_DIR)
    set(ERPC_DIR ${CMAKE_SOURCE_DIR}/deps/eRPC)
endif()
message("ERPC dir is ${ERPC_DIR}")

# ---- Fetch vendored submodules automatically if missing ----
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${CMAKE_SOURCE_DIR}/.git")
    execute_process(
            COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            RESULT_VARIABLE GIT_SUBMODULE_RESULT
    )
    if(NOT GIT_SUBMODULE_RESULT EQUAL "0")
        message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMODULE_RESULT}")
    endif()
endif()

# ---- Build vendored eRPC automatically if it hasn't been built yet ----
# eRPC is built via deps/Makefile (which knows the TRANSPORT=infiniband flag to
# match ERPC_INFINIBAND below) rather than duplicating that logic here. This only
# runs once: it builds nothing more on later configures/builds once liberpc.a and
# config.h exist. If you change deps/eRPC yourself, `make -C deps clean` (or delete
# deps/eRPC/build) to force a rebuild.
find_program(MAKE_EXECUTABLE NAMES make gmake REQUIRED)
set(ERPC_LIB ${ERPC_DIR}/build/liberpc.a)
set(ERPC_CONFIG_HEADER ${ERPC_DIR}/build/src/config.h)
add_custom_command(
        OUTPUT ${ERPC_LIB} ${ERPC_CONFIG_HEADER}
        COMMAND ${MAKE_EXECUTABLE} -C ${CMAKE_SOURCE_DIR}/deps eRPC
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building vendored eRPC dependency (deps/eRPC) - this only happens once and may take a while"
        VERBATIM
)
add_custom_target(erpc_dep DEPENDS ${ERPC_LIB} ${ERPC_CONFIG_HEADER})

# ---- Dependencies ----
find_package(Threads REQUIRED)
find_library(NUMA_LIB numa REQUIRED)
find_package(ZLIB QUIET)

# Build vendored HdrHistogram (used by benchmark clients).
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
add_subdirectory(deps/HdrHistogram_c EXCLUDE_FROM_ALL)

# ---- Sources ----
file(GLOB ZIPLOG_SRC "lib/*/*.cpp")
file(GLOB BIN_SRC "bin/*.cpp")

# ---- Core static library ----
add_library(ziplog STATIC ${ZIPLOG_SRC})
set_target_properties(ziplog PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        OUTPUT_NAME "zip"
        INTERPROCEDURAL_OPTIMIZATION_RELEASE ON
)

target_include_directories(ziplog
        PUBLIC
        "${CMAKE_SOURCE_DIR}/include"
        "${CMAKE_SOURCE_DIR}/deps/cxxopts/include"
        "${CMAKE_SOURCE_DIR}/deps/HdrHistogram_c/include"
        ${ERPC_DIR}/src
        ${ERPC_DIR}/build/src
        ${ERPC_DIR}/third_party/asio/include
)

target_link_libraries(ziplog
        PUBLIC
        Threads::Threads
        ${ERPC_DIR}/build/liberpc.a
        ${NUMA_LIB}
        ibverbs
        numa
)

target_compile_definitions(ziplog PUBLIC ERPC_INFINIBAND=true)

# ziplog's own sources include eRPC headers directly, so make sure eRPC (and its
# generated config.h) is built before ziplog starts compiling, not just before linking.
add_dependencies(ziplog erpc_dep)

# Common link set for executables.
set(COMMON_LIBS ziplog Threads::Threads ${NUMA_LIB} ${ERPC_DIR}/build/liberpc.a)

# ---- bin/* executables ----
foreach(BIN_SOURCE ${BIN_SRC})
    get_filename_component(BIN_NAME ${BIN_SOURCE} NAME_WE)
    add_executable(${BIN_NAME} ${BIN_SOURCE})
    target_link_libraries(${BIN_NAME} PRIVATE ${COMMON_LIBS})
    add_dependencies(${BIN_NAME} erpc_dep)
    set_target_properties(${BIN_NAME} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/out/bin"
            INTERPROCEDURAL_OPTIMIZATION_RELEASE ON
    )
endforeach()

# ---- apps/append ----
add_executable(append_client apps/append/client.cpp)
target_include_directories(append_client PRIVATE "${CMAKE_SOURCE_DIR}/apps/pc/include")

target_link_libraries(append_client PRIVATE ${COMMON_LIBS} hdr_histogram_static)
add_dependencies(append_client erpc_dep)
set_target_properties(append_client PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/out/apps/append"
        OUTPUT_NAME "client"
        INTERPROCEDURAL_OPTIMIZATION_RELEASE ON
)

# ---- apps/pc ----
add_executable(producer_consumer apps/pc/producer.cpp)
target_link_libraries(producer_consumer PRIVATE ${COMMON_LIBS} hdr_histogram_static)
add_dependencies(producer_consumer erpc_dep)
set_target_properties(producer_consumer PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/out/apps/pc"
        OUTPUT_NAME "pc"
        INTERPROCEDURAL_OPTIMIZATION_RELEASE ON
)

# zlib is needed when hdr log encoding is enabled.
if(TARGET ZLIB::ZLIB)
    target_link_libraries(append_client PRIVATE ZLIB::ZLIB)
endif()

# Convenience target similar to old "lib" goal.
add_custom_target(lib_only DEPENDS ziplog)

# Keep an old-style dependency cleanup hook.
add_custom_target(clean_deps
        COMMAND ${MAKE_EXECUTABLE} -C ${CMAKE_SOURCE_DIR}/deps clean
        COMMENT "Cleaning external dependencies"
)

if(ZIPLOG_BUILD_TESTS AND EXISTS "${CMAKE_SOURCE_DIR}/tests/CMakeLists.txt")
    enable_testing()
    add_subdirectory(tests)
endif()
