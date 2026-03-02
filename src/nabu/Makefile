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

LIBRARY = nabu

OBJS += access.o
OBJS += access_imp.o
OBJS += access_table.o
OBJS += address.o
OBJS += async_status.o
OBJS += branch.o
OBJS += branch_imp.o
OBJS += cache.o
OBJS += column_name.o
OBJS += config.o
OBJS += context.o
OBJS += database.o
OBJS += database_imp.o
OBJS += data_file.o
OBJS += exception.o
OBJS += filenamer.o
OBJS += garbage.o
OBJS += interface_types.o
OBJS += io_op.o
OBJS += io_op_imp_read.o
OBJS += io_op_imp_write.o
OBJS += journal.o
OBJS += journal_thread.o
OBJS += label_file.o
OBJS += leak_detector.o
OBJS += metadata.o
OBJS += metadata_map.o
OBJS += metadata_table.o
OBJS += metakey.o
OBJS += private_types.o
OBJS += profiler.o
OBJS += record.o
OBJS += tag.o
OBJS += tag_imp.o
OBJS += table_address.o
OBJS += time_path.o
OBJS += time_scheme.o
OBJS += trace.o
OBJS += version.o
OBJS += wcm.o

OEM_INCLUDE_PATHS += /opt/$(ARCH)-libuuid/include
#OEM_INCLUDE_PATHS += /opt/$(ARCH)-openssl/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-xxhash/include
OEM_INCLUDE_PATHS += /opt/$(ARCH)-zlib/include

GITROOT=$(shell git rev-parse --show-toplevel)
GICMDIR=$(GITROOT)/common/gicm

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

GITROOT=$(shell git rev-parse --show-toplevel)
GICMDIR=$(GITROOT)/common/gicm

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
