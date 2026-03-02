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

LIBRARY = mcor

OBJS += arg_map.o
OBJS += atfile.o
OBJS += bigtime.o
OBJS += binary.o
OBJS += detrend.o
OBJS += directory.o
OBJS += extrema.o
OBJS += field_matcher.o
OBJS += hash_md5.o
OBJS += istring_map.o
OBJS += json_config.o
OBJS += json_safe_file.o
OBJS += json_strutil.o
OBJS += json_time.o
OBJS += mcondition.o
OBJS += mcounter.o
OBJS += menvironment.o
OBJS += mexception.o
OBJS += mfile.o
OBJS += mlinux.o
OBJS += mpipe.o
OBJS += mpopen.o
OBJS += mtimer.o
OBJS += mmutex.o
OBJS += mthread.o
OBJS += muuid.o
OBJS += poller.o
OBJS += profiler.o
OBJS += random.o
OBJS += rwlock.o
OBJS += safe_file.o
OBJS += serial.o
OBJS += string_file.o
OBJS += string_map.o
OBJS += strutil.o
OBJS += thread_analysis.o
OBJS += mtime.o
OBJS += timerange.o
OBJS += timespec.o
OBJS += url.o

# for MD5 support used in safe_file
OEM_INCLUDE_PATHS += /opt/$(ARCH)-openssl/include
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



