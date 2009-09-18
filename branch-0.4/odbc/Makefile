#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This makefile depends on 4 environment variables to funtion correctly:
# a) WORD_SIZE
# b) THRIFT_HOME
# c) HIVE_ROOT
# d) BASE_DIR
# All these are passed by build.xml.
#

WORD_SIZE ?= 64
ifeq ($(WORD_SIZE),32)
  ARCH_FLAGS = -m32 -DARCH32
else
  ARCH_FLAGS = -m64 -DARCH64
endif

AR = ar
ARFLAGS = rcs
ARXFLAGS = -x
CC = gcc
CFLAGS = -Wall -g -fPIC
CXX = g++
CXXFLAGS = -Wall -g -fPIC
LD = g++
LDFLAGS = -shared
INSTALL = /usr/bin/install -c
SHELL = /bin/sh
LIBTOOL = $(SHELL) /usr/bin/libtool
LINK = ln -sf

BUILD_DIR = $(HIVE_ROOT)/build
ODBC_BUILD_DIR = $(BUILD_DIR)/odbc
OBJ_SERVICE_BUILD_DIR = $(BUILD_DIR)/service/objs
OBJ_FB303_BUILD_DIR = $(BUILD_DIR)/service/fb303/objs
OBJ_METASTORE_BUILD_DIR = $(BUILD_DIR)/metastore/objs
OBJ_ODBC_BUILD_DIR = $(ODBC_BUILD_DIR)/objs
LIB_ODBC_BUILD_DIR = $(ODBC_BUILD_DIR)/lib
TEST_ODBC_BUILD_DIR = $(ODBC_BUILD_DIR)/test
THRIFT_INCLUDE_PATH = $(HIVE_ROOT)/service/include/thrift
INSTALL_PATH = /usr/local
INSTALL_LIB_PATH = $(INSTALL_PATH)/lib
INSTALL_INCLUDE_PATH = $(INSTALL_PATH)/include

LIB_NAME = hiveclient
SHLIB_VERSION = 1.0.0
SO_LINK_NAME = lib$(LIB_NAME).so
SO_NAME = $(SO_LINK_NAME).$(SHLIB_VERSION)
SO_LINK_TARGET = $(LIB_ODBC_BUILD_DIR)/$(SO_LINK_NAME)
SO_TARGET = $(LIB_ODBC_BUILD_DIR)/$(SO_NAME) 
SO_INSTALL_LINK_TARGET = $(INSTALL_LIB_PATH)/$(SO_LINK_NAME)
SO_INSTALL_TARGET = $(INSTALL_LIB_PATH)/$(SO_NAME)
AR_NAME = lib$(LIB_NAME).a
AR_TARGET = $(LIB_ODBC_BUILD_DIR)/$(AR_NAME)
AR_INSTALL_TARGET = $(INSTALL_LIB_PATH)/$(AR_NAME)

SERVICE_SRC_DIR = $(HIVE_ROOT)/service/src/gen-cpp
SERVICE_SOURCES = ThriftHive.cpp \
                  hive_service_constants.cpp \
                  hive_service_types.cpp
SERVICE_OBJS = $(addprefix $(OBJ_SERVICE_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(SERVICE_SOURCES))))

FB303_SRC_DIR = $(THRIFT_INCLUDE_PATH)/fb303
FB303_SOURCES = FacebookService.cpp \
                fb303_constants.cpp \
                fb303_types.cpp
FB303_OBJS = $(addprefix $(OBJ_FB303_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(FB303_SOURCES))))

METASTORE_SRC_DIR = $(HIVE_ROOT)/metastore/src/gen-cpp
METASTORE_SOURCES = ThriftHiveMetastore.cpp \
                    hive_metastore_constants.cpp \
                    hive_metastore_types.cpp
METASTORE_OBJS = $(addprefix $(OBJ_METASTORE_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(METASTORE_SOURCES))))

ODBC_SRC_DIR = $(BASE_DIR)/src/cpp
ODBC_SOURCES = hiveclient.cpp \
               HiveResultSet.cpp \
               HiveColumnDesc.cpp \
               HiveRowSet.cpp \
               hiveclienthelper.cpp
ODBC_OBJS = $(addprefix $(OBJ_ODBC_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(ODBC_SOURCES))))

ODBC_TEST_SRC_DIR = $(BASE_DIR)/src/test
ODBC_TEST_DATA_DIR = $(BASE_DIR)/testdata
TEST_FLAGS = -DTEST_DATA_DIR=$(ODBC_TEST_DATA_DIR)
HIVE_CLIENT_TEST = $(TEST_ODBC_BUILD_DIR)/HiveClientTestC

INCLUDE_PATHS = -I$(THRIFT_INCLUDE_PATH) \
                -I$(FB303_SRC_DIR) \
                -I$(SERVICE_SRC_DIR) \
                -I$(METASTORE_SRC_DIR) \
                -I$(ODBC_SRC_DIR)

INSTALL_HEADERS = hiveclient.h \
                  hiveconstants.h
HEADER_SOURCES = $(addprefix $(ODBC_SRC_DIR)/, $(INSTALL_HEADERS))
HEADER_TARGETS = $(INSTALL_INCLUDE_PATH)/hiveclient.h \
                  $(INSTALL_INCLUDE_PATH)/hiveconstants.h

LIB_THRIFT_DIR = $(THRIFT_HOME)/lib
LIB_THRIFT_ADD = -L$(LIB_THRIFT_DIR) -lthrift
LIB_THRIFT_AR = $(LIB_THRIFT_DIR)/libthrift.a


all:: $(AR_TARGET) $(SO_TARGET) $(HIVE_CLIENT_TEST)

$(AR_TARGET): $(FB303_OBJS) $(METASTORE_OBJS) $(SERVICE_OBJS) $(ODBC_OBJS)
	if test -z '$(THRIFT_HOME)'; then echo 'THRIFT_HOME directory?'; exit 1; else exit 0; fi
	mkdir -p $(LIB_ODBC_BUILD_DIR)
	$(AR) $(ARXFLAGS) $(LIB_THRIFT_AR) #Extract thrift archive
	$(AR) $(ARFLAGS) $@ $+ *.o #Include all object files into new archive
	rm *.o #Remove extracted archive object files

$(SO_TARGET): $(FB303_OBJS) $(METASTORE_OBJS) $(SERVICE_OBJS) $(ODBC_OBJS)
	if test -z '$(THRIFT_HOME)'; then echo 'THRIFT_HOME directory?'; exit 1; else exit 0; fi
	mkdir -p $(LIB_ODBC_BUILD_DIR)
	$(LD) $(ARCH_FLAGS) $(LDFLAGS) $+ $(LIB_THRIFT_ADD) -o $@ \
        && $(LINK) $(SO_NAME) $(SO_LINK_TARGET)


$(OBJ_SERVICE_BUILD_DIR)/%.o: $(SERVICE_SRC_DIR)/%.cpp
	mkdir -p $(OBJ_SERVICE_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(OBJ_FB303_BUILD_DIR)/%.o: $(FB303_SRC_DIR)/%.cpp
	mkdir -p $(OBJ_FB303_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(OBJ_METASTORE_BUILD_DIR)/%.o: $(METASTORE_SRC_DIR)/%.cpp
	mkdir -p $(OBJ_METASTORE_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(OBJ_ODBC_BUILD_DIR)/%.o: $(ODBC_SRC_DIR)/%.cpp
	mkdir -p $(OBJ_ODBC_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(HIVE_CLIENT_TEST): $(SO_TARGET) $(ODBC_TEST_SRC_DIR)/hiveclienttest.c
	mkdir -p $(TEST_ODBC_BUILD_DIR)
	$(CC) $(CFLAGS) $(ARCH_FLAGS) $(TEST_FLAGS) $(INCLUDE_PATHS) $(ODBC_TEST_SRC_DIR)/hiveclienttest.c -L$(LIB_ODBC_BUILD_DIR) -L$(LIB_THRIFT_DIR) -l$(LIB_NAME) -lthrift -o $@


install: $(AR_TARGET) $(SO_TARGET)
	test -z "$(INSTALL_LIB_PATH)" || mkdir -p -- "$(INSTALL_LIB_PATH)"
	$(LIBTOOL) --mode=install $(INSTALL) '$(AR_TARGET)' '$(AR_INSTALL_TARGET)'
	$(LIBTOOL) --mode=install $(INSTALL) '$(SO_TARGET)' '$(SO_INSTALL_TARGET)' \
	&& $(LINK) $(SO_NAME) $(SO_INSTALL_LINK_TARGET)
	PATH="$PATH:/sbin" ldconfig -n $(INSTALL_LIB_PATH)
	test -z "$(INSTALL_INCLUDE_PATH)" || mkdir -p -- "$(INSTALL_INCLUDE_PATH)"
	$(INSTALL) -m 644 $(HEADER_SOURCES) $(INSTALL_INCLUDE_PATH)

uninstall:
	$(LIBTOOL) --mode=uninstall rm -f '$(AR_INSTALL_TARGET)' '$(SO_INSTALL_LINK_TARGET)' '$(SO_INSTALL_TARGET)'
	rm -f $(HEADER_TARGETS)

clean:
	rm -rf $(ODBC_BUILD_DIR) $(OBJ_SERVICE_BUILD_DIR) $(OBJ_FB303_BUILD_DIR) $(OBJ_METASTORE_BUILD_DIR)

test: $(AR_TARGET) $(SO_TARGET) $(HIVE_CLIENT_TEST)
	LD_LIBRARY_PATH=$(LIB_ODBC_BUILD_DIR):$(LIB_THRIFT_DIR):$(LD_LIBRARY_PATH) $(HIVE_CLIENT_TEST)
