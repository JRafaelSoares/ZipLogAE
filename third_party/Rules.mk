# Build ziplog

d := $(dir $(lastword $(MAKEFILE_LIST)))

ziplog_src := $(addprefix $(d), ziplog/src/)
ziplog_network := $(addprefix $(ziplog_src), network)
ziplog_util := $(addprefix $(ziplog_src), util)
ziplog_client := $(addprefix $(ziplog_src), client)

ziplog_obj := $(addprefix $(o), ziplog/src/)
ziplog_network_obj := $(addprefix $(ziplog_obj), network)
ziplog_util_obj := $(addprefix $(ziplog_obj), util)
ziplog_client_obj := $(addprefix $(ziplog_obj), client)
LIB-ziplog := $(ziplog_network_obj)/buffer.o $(ziplog_network_obj)/manager.o $(ziplog_network_obj)/send_queue.o $(ziplog_network_obj)/recv_queue.o \
             $(ziplog_util_obj)/util.o $(ziplog_client_obj)/client.o

# ziplog's vendored sources use a .cpp extension, unlike the rest of this
# build (which uses .cc), so they can't go through the project-wide %.cc
# SRCS/OBJS machinery. Compile them via static pattern rules scoped to just
# these objects instead.
$(ziplog_network_obj)/buffer.o $(ziplog_network_obj)/manager.o $(ziplog_network_obj)/send_queue.o $(ziplog_network_obj)/recv_queue.o: $(ziplog_network_obj)/%.o: $(ziplog_network)/%.cpp
	$(call compilecxx,CC,)

$(ziplog_util_obj)/util.o: $(ziplog_util_obj)/%.o: $(ziplog_util)/%.cpp
	$(call compilecxx,CC,)

$(ziplog_client_obj)/client.o: $(ziplog_client_obj)/%.o: $(ziplog_client)/%.cpp
	$(call compilecxx,CC,)
