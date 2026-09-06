##############################################
#      Makefile for the ziplog project       #
##############################################

# Set the compiler flags
CXX := clang++-16
OPTS := -O3 -mavx -flto
override CXXFLAGS += -std=c++20 -Wall -Wextra $(OPTS) -Isrc -Ideps/cxxopts/include -Ideps/HdrHistogram_c/include
override LDFLAGS += deps/jemalloc/lib/libjemalloc.a deps/HdrHistogram_c/build/src/libhdr_histogram_static.a -lpthread -libverbs -lz -lnuma

# List the source files
COMMON_SRC := $(wildcard src/network/*.cpp src/util/*.cpp)
CLIENT_SRC := $(wildcard src/client/*.cpp) $(COMMON_SRC)
SUBSCRIBER_SRC := $(wildcard src/subscriber/*.cpp) $(COMMON_SRC)
STORAGE_SRC := $(wildcard src/storage/*.cpp) $(COMMON_SRC)
ORDER_SRC := $(wildcard src/order/*.cpp) $(COMMON_SRC)
ALL_SRC := $(wildcard src/*/*.cpp)

CLIENT_NOMAIN := $(filter-out src/client/main.cpp, $(wildcard src/client/*.cpp))
SUBSCRIBER_NOMAIN := $(filter-out src/subscriber/main.cpp, $(wildcard src/subscriber/*.cpp))
LOCK_SRC := $(wildcard src/lock_service/*.cpp) $(COMMON_SRC) $(CLIENT_NOMAIN) $(SUBSCRIBER_NOMAIN)

# List the object files
BUILD_DIR := build
CLIENT_O := $(CLIENT_SRC:%.cpp=$(BUILD_DIR)/%.o)
SUBSCRIBER_O := $(SUBSCRIBER_SRC:%.cpp=$(BUILD_DIR)/%.o)
LOCK_O := $(LOCK_SRC:%.cpp=$(BUILD_DIR)/%.o)
STORAGE_O := $(STORAGE_SRC:%.cpp=$(BUILD_DIR)/%.o)
ORDER_O := $(ORDER_SRC:%.cpp=$(BUILD_DIR)/%.o)
DEPS := $(ALL_SRC:%.cpp=$(BUILD_DIR)/%.d)

# Default rule to build everything
all: client storage subscriber lock_service order
client: $(BUILD_DIR)/client
subscriber: $(BUILD_DIR)/subscriber
lock_service: $(BUILD_DIR)/lock_service
storage: $(BUILD_DIR)/storage
order: $(BUILD_DIR)/order

# Rules for the dependencies
deps:
	@$(MAKE) -C deps

clean_deps:
	@$(MAKE) clean -C deps

# Include all .d files
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(MAKECMDGOALS),deps)
ifneq ($(MAKECMDGOALS),clean_deps)
-include $(DEPS)
endif
endif
endif

# Rules to build the object and dependency files
$(BUILD_DIR)/%.o:
	@mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) $< -o $@

$(BUILD_DIR)/%.d: %.cpp
	@mkdir -p $(@D)
	@$(CXX) -MM -MT $(@:.d=.o) $(CXXFLAGS) $< -o $@

# Rules to make the binaries
$(BUILD_DIR)/storage: $(STORAGE_O)
$(BUILD_DIR)/client: $(CLIENT_O)
$(BUILD_DIR)/subscriber: $(SUBSCRIBER_O)
$(BUILD_DIR)/lock_service: $(LOCK_O)
$(BUILD_DIR)/order: $(ORDER_O)
$(BUILD_DIR)/storage $(BUILD_DIR)/subscriber $(BUILD_DIR)/lock_service $(BUILD_DIR)/client $(BUILD_DIR)/order:
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

clean:
	rm -rf $(BUILD_DIR)

# Ignore the spurious rules
.PHONY: all clean client storage order subscriber lock_service deps clean_deps
