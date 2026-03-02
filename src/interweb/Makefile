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

LIBRARY = interweb

OBJS += http_headers.o
OBJS += http_parameters.o
OBJS += javascript.o
OBJS += request.o

OEM_INCLUDE_PATHS += /opt/$(ARCH)-curl/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-jsoncpp/include

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



