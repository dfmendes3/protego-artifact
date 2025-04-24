# Makefile for silo-client
ROOT_PATH?=../caladan/
include $(ROOT_PATH)/build/shared.mk

BW_LIBS = $(ROOT_PATH)/breakwater/libbw.a

silo-client_src = silo-client.cc
silo-client_obj = $(silo-client_src:.cc=.o)

librt_libs = $(ROOT_PATH)/bindings/cc/librt++.a
libbw_libs = $(ROOT_PATH)/breakwater/bindings/cc/libbw++.a
INC += -I$(ROOT_PATH)/breakwater/inc
INC += -I$(ROOT_PATH)/bindings
INC += -I$(ROOT_PATH)/breakwater/bindings/cc/inc

RUNTIME_LIBS := $(RUNTIME_LIBS) $(BW_LIBS) -lnuma

# must be first
all: silo-client

silo-client : $(silo-client_obj) $(librt_libs) $(libbw_libs) $(RUNTIME_DEPS)
	$(LDXX) -o $@ $(LDFLAGS) $(silo-client_obj) $(librt_libs) \
	$(libbw_libs) $(RUNTIME_LIBS)

# general build rules for all targets
src = $(silo-client_src)
obj = $(src:.cc=.o)
dep = $(obj:.o=.d)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)   # include all dep files in the makefile
endif

# rule to generate a dep file by using the C preprocessor
# (see man cpp for details on the -MM and -MT options)
%.d: %.cc
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	rm -f $(obj) $(dep) silo-client