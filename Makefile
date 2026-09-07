##############################################
#      Makefile for the ziplog project       #
##############################################

ZIPKAT_CXXFLAGS := -DCOLOCATED_ZIPKAT -Isrc/app/zipkat -Isrc/app/zipkat/store #-DMEASURE_LOG_ITERATE #-DZIP_MEASURE

# Set the compiler flags
CXX := clang++
override CXXFLAGS += -g -std=c++17 -Wall -O3 -mavx -flto -Isrc -Ideps/cxxopts/include -Ideps/HdrHistogram_c/include ${ZIPKAT_CXXFLAGS}
override LDFLAGS += deps/jemalloc/lib/libjemalloc.a deps/HdrHistogram_c/build/src/libhdr_histogram_static.a -lpthread -libverbs

# List the source files
APP_SRC := $(shell find src/app/ -name "*.cpp") $(shell find src/app -name "*.cc")
COMMON_SRC := $(wildcard src/network/*.cpp src/util/*.cpp)
CLIENT_SRC := $(wildcard src/client/*.cpp) $(COMMON_SRC)
SUBSCRIBER_SRC := $(wildcard src/subscriber/*.cpp) $(COMMON_SRC)
STORAGE_SRC := $(wildcard src/storage/*.cpp) $(COMMON_SRC) $(APP_SRC)
ORDER_SRC := $(wildcard src/order/*.cpp) $(COMMON_SRC)
ALL_SRC := $(shell find src/ -name "*.cpp")

# List the object files
BUILD_DIR := build
CLIENT_O := $(CLIENT_SRC:%.cpp=$(BUILD_DIR)/%.o)
SUBSCRIBER_O := $(SUBSCRIBER_SRC:%.cpp=$(BUILD_DIR)/%.o)
STORAGE_O := $(STORAGE_SRC:%.cpp=$(BUILD_DIR)/%.o)
ORDER_O := $(ORDER_SRC:%.cpp=$(BUILD_DIR)/%.o)
DEPS := $(ALL_SRC:%.cpp=$(BUILD_DIR)/%.d)

# Default rule to build everything
all: client storage order
#all: client storage subscriber order
client: $(BUILD_DIR)/client
client_so: $(BUILD_DIR)/libziplog_client.so
subscriber: $(BUILD_DIR)/subscriber
subscriber_so: $(BUILD_DIR)/libziplog_subscriber.so
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
$(BUILD_DIR)/order: $(ORDER_O)
$(BUILD_DIR)/storage $(BUILD_DIR)/client $(BUILD_DIR)/subscriber $(BUILD_DIR)/order:
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

clean:
	rm -rf $(BUILD_DIR)

# Ignore the spurious rules
.PHONY: all clean client storage order subscriber deps clean_deps
