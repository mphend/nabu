# Makefile
#
# Copyright notice:
# Free use is permitted under the guidelines and in accordance with the most current
# version of the MIT License.
# https://www.opensource.org/licenses/MIT
#

ifndef ARCH
ARCH = x86
endif

APP = extents

OBJS += extents.o

LIBS += nabu_client
LIBS += nabu
LIBS += interweb
LIBS += mcor

OEM_INCLUDE_PATHS += /opt/$(ARCH)-jsoncpp/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-jsoncpp/lib
OEM_LIBRARIES += jsoncpp

OEM_INCLUDE_PATHS += /opt/$(ARCH)-libuuid/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-libuuid/lib
OEM_LIBRARIES += uuid

OEM_INCLUDE_PATHS += /opt/$(ARCH)-curl/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-curl/lib
OEM_LIBRARIES += curl

OEM_INCLUDE_PATHS += /opt/$(ARCH)-openssl/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-openssl/lib64
OEM_LIBRARIES += ssl
OEM_LIBRARIES += crypto

OEM_INCLUDE_PATHS += /opt/$(ARCH)-xxhash/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-xxhash/lib
OEM_LIBRARIES += xxhash

OEM_INCLUDE_PATHS += /opt/$(ARCH)-zlib/include
OEM_LIBRARY_PATHS += /opt/$(ARCH)-zlib/lib
OEM_LIBRARIES += z

GITROOT=$(shell git rev-parse --show-toplevel)

ifndef OUTPUT_DIR
	OUTPUT_DIR=${ARCH}
endif

# tool prefix
ifeq (${ARCH},arm)
 	PREFIX=arm-linux-
else ifeq (${ARCH},rpi)
	PREFIX=arm-linux-gnueabihf-
endif

# uncomment this to build debug
#GDB = -g

ALL : ${OUTPUT_DIR} $(APP)

# translate each library ala:
#    mcor
# to
#    mcor/x86/libmcor.a
LIB_DEPENDENCIES = $(foreach lib, $(LIBS),$(lib)/$(OUTPUT_DIR)/lib$(lib).a)

clean: libs
	rm -rf ${OUTPUT_DIR}


GCC = ${PREFIX}gcc
AR = ${PREFIX}ar

${OUTPUT_DIR}:
	echo ARCH = $(ARCH)
	@mkdir ${OUTPUT_DIR}

ifneq ($(MAKECMDGOALS),clean)
include $(foreach obj,$(OBJS:.o=.d),$(OUTPUT_DIR)/$(obj))
include ../../Makefile.dependencies
endif

${OUTPUT_DIR}/%.o : %.cpp
	${GCC} -c $(GDB) $(CFLAGS) -O2 $(foreach dir,$(OEM_INCLUDE_PATHS),-I $(dir)) -I ../../ -std=c++0x $(CPPFLAGS) $< -o $@

libs:
	@echo building $@
	@$(foreach lib,$(LIBS),cd ../../$(lib); make ARCH=${ARCH} OUTPUT_DIR=${OUTPUT_DIR} $(MAKECMDGOALS) || exit;)

${APP}: libs $(addprefix ${OUTPUT_DIR}/,${OBJS})
	@echo $@
	${GCC} \
	$(foreach lib,$(LIBS),-L ../../$(lib)/${OUTPUT_DIR}) \
	$(foreach lib,$(OEM_LIBRARY_PATHS),-L $(lib)) \
	$(addprefix ${OUTPUT_DIR}/,${OBJS}) \
	$(foreach lib,$(LIBS),-l$(lib)) \
	$(foreach lib,$(OEM_LIBRARIES),-l$(lib)) \
	$(RT) -lstdc++ -lpthread -lm -ldl $ \
	${FINAL_LINK_ARGS} \
	-o ${OUTPUT_DIR}/$@



