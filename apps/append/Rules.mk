##############################################
#    Makefile for the append benchmark app   #
##############################################

CURR_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
BINS_SRC += $(addprefix $(CURR_DIR), client.cpp)

# Add the dependencies for the binaries
$(BINS_DIR)/$(CURR_DIR)client: deps/HdrHistogram_c/build/src/libhdr_histogram_static.a -lz
