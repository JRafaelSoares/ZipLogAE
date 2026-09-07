##############################################
#  Makefile for the respective dependencies  #
##############################################

# List all the dependencies here
VENDOR := HdrHistogram_c jemalloc

# Build all dependencies
all: $(VENDOR)

# Build the jemalloc library
jemalloc:
	cd jemalloc; ./autogen.sh
	$(MAKE) -C jemalloc

# Build the histogram library using cmake
HdrHistogram_c:
	mkdir -p HdrHistogram_c/build
	cmake -S HdrHistogram_c -B HdrHistogram_c/build
	$(MAKE) -C HdrHistogram_c/build

clean:
	rm -rf HdrHistogram_c/build
	$(MAKE) -C jemalloc clean

# Ignore the spurious rules
.PHONY: all clean $(VENDOR)
