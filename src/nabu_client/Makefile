# Makefile
#
# Copyright notice:
# Free use is permitted under the guidelines and in accordance with the most current
# version of the MIT License.
# https://www.opensource.org/licenses/MIT
#

ifndef ARCH
	ARCH=x86
endif

LIBRARY = nabu_client

OBJS += access.o
OBJS += branch.o
OBJS += database.o
OBJS += iop.o
OBJS += tag.o


OEM_INCLUDE_PATHS += /opt/$(ARCH)-libuuid/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-curl/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-openssl/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-xxhash/include

GITROOT=$(shell git rev-parse --show-toplevel)

CFLAGS += -Wno-deprecated-declarations

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
ALL : ${OUTPUT_DIR} ${OUTPUT_DIR}/lib${LIBRARY}.a

clean:
	rm -rf ${OUTPUT_DIR}

install:


# tools
GCC = ${PREFIX}gcc
AR = ${PREFIX}ar

${OUTPUT_DIR}:
	@mkdir ${OUTPUT_DIR}


SRCS=$(OBJS:.o=)

ifneq ($(MAKECMDGOALS),clean)
-include $(foreach src,$(SRCS),$(OUTPUT_DIR)/$(src).d)
-include Makefile.dependencies
endif

${OUTPUT_DIR}/%.o : %.cpp
	@echo $@
	${GCC} $(GDB) -std=c++0x -c $(CFLAGS) $(foreach dir,$(OEM_INCLUDE_PATHS),-I $(dir)) -I ../ $(CPPFLAGS) $< -o $@

${OUTPUT_DIR}/%.o : %.c
	@echo $@
	${GCC} -c $(CFLAGS) $(foreach dir,$(OEM_INCLUDE_PATHS),-I $(dir)) -I ../ $(CPPFLAGS) $< -o $@

${OUTPUT_DIR}/lib${LIBRARY}.a: $(addprefix ${OUTPUT_DIR}/,${OBJS})
	@echo $@
	${AR} -r $@ $(addprefix ${OUTPUT_DIR}/,${OBJS})



