##############################################
#      Makefile for the ziplog project       #
##############################################

# Set the basic variables
BUILD_DIR := build
BINS_DIR := $(BUILD_DIR)/out
CXX := clang++-20
AR := llvm-ar-20
OPTS := -O3 -mavx -flto
INCLUDES := -Iinclude -Ideps/cxxopts/include -Ideps/HdrHistogram_c/include
CXXFLAGS += -std=c++20 -Wall -Wextra -Wno-missing-field-initializers $(OPTS)
LDFLAGS += -lpthread -libverbs -lnuma

# Create the default rule to run
all:

# List the ziplog source files
ZIPLOG_SRC := $(wildcard lib/*/*.cpp)
BINS_SRC := $(wildcard bin/*.cpp)

# Include the apps
-include apps/*/Rules.mk

# Goals for the static library, binaries and clean up
BINS := $(BINS_SRC:%.cpp=$(BINS_DIR)/%)
LIB := $(BUILD_DIR)/lib/libzip.a
CLEAN := clean clean_deps

# List the object files
ALL_SRC := $(ZIPLOG_SRC) $(APPS_SRC) $(BINS_SRC)
ZIPLOG_O := $(ZIPLOG_SRC:%.cpp=$(BUILD_DIR)/%.o)
DEPS := $(ALL_SRC:%.cpp=$(BUILD_DIR)/%.d)

# Default rule to build everything
all: $(BINS)
lib: $(LIB)

# Special rules for dependencies
$(BUILD_DIR)/storage: deps/jemalloc/lib/libjemalloc.a

# Rules for the dependencies
deps/HdrHistogram_c/build/src/libhdr_histogram_static.a:
	@$(MAKE) -C deps HdrHistogram_c
deps/jemalloc/lib/libjemalloc.a:
	@$(MAKE) -C deps jemalloc
clean_deps:
	@$(MAKE) -C deps clean

# Include all .d files if we have only the default goal
ifeq (,$(MAKECMDGOALS))
-include $(DEPS)
else
# Include all .d files unless the only rules we
# have given are clean or clean_deps
ifneq (,$(filter-out $(CLEAN),$(MAKECMDGOALS)))
-include $(DEPS)
endif
endif

# Rules to build the object and dependency files
$(BUILD_DIR)/%.o:
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) $(INCLUDES) $< -o $@

$(BUILD_DIR)/%.d: %.cpp
	@mkdir -p $(@D)
	@$(CXX) -MM -MT $(@:.d=.o) $(CXXFLAGS) $(INCLUDES) $< -o $@

$(LIB): $(ZIPLOG_O)
	@mkdir -p $(@D)
	$(AR) rcs $@ $^

# Rules to make the binaries
$(BINS_SRC:%.cpp=$(BINS_DIR)/%): $(BINS_DIR)/%: $(BUILD_DIR)/%.o $(LIB)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

clean:
	rm -rf $(BUILD_DIR)

# Ignore the spurious rules
.PHONY: all lib $(CLEAN)
