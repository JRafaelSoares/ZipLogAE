d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), retwisClient.cc)

OBJS-all-clients := $(LIB-message) $(OBJS-meerkatstore-client) $(OBJS-meerkatstore-leader-client)

$(d)retwisClient: $(OBJS-all-clients) $(o)retwisClient.o

BINS += $(d)retwisClient
