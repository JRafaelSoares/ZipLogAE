#!/bin/bash
set -e

cd "$(dirname "$0")"/deps/eRPC/

if [ -d build ]; then
  # A stale build/ owned by another user (e.g. leftover root-owned artifacts)
  # can't be rm -rf'd without privilege escalation; move it aside instead.
  rm -rf build 2>/dev/null || mv build "build.stale.$(date +%s)"
fi
mkdir build
cd build

# Two compiler-toolchain mismatches between eRPC's vendored CMakeLists.txt
# and this container's GCC 13 / CMake 3.28, worked around here instead of
# editing eRPC's source:
#  - configure_file(src/config.h.in src/config.h) writes into the build
#    tree's src/ (CMAKE_CURRENT_BINARY_DIR), but only the source tree's src/
#    is on the include path -> add the build tree's src/ explicitly.
#  - Several headers use std::array without including <array>, relying on
#    an older libstdc++ transitively pulling it in via <tuple>/boost. GCC
#    13's libstdc++ no longer does -> force-include <array>, the same
#    technique eRPC's own CMakeLists.txt uses for MICA's config.h.
cmake .. -DPERF=OFF -DTRANSPORT=infiniband -DROCE=on \
  -DCMAKE_CXX_FLAGS="-I$(pwd)/src -include array"

# Build only the erpc library target (liberpc.a), which is all the outer
# retwis Makefile links against (see ERPC_LDFLAGS_RAW/_DPDK). eRPC's own
# bundled unit tests ("make all"'s other targets) use gtest-internal APIs
# (testing::internal::ColoredPrintf/COLOR_*) that this container's newer
# libgtest-dev (1.14.0) removed; fixing that would require editing eRPC's
# test sources, which is out of scope here.
make -j erpc