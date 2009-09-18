/**************************************************************************//**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************
 *
 * @file thriftserverconstants.h
 * @brief Provides constants necessary for Hive Client interaction with Hive Server
 *
 *****************************************************************************/


#ifndef __thrift_server_constants_h__
#define __thrift_server_constants_h__

/// Maximum number of characters needed to display any field
static const int MAX_DISPLAY_SIZE = 334;
/// Maximum number of bytes needed to store any field
static const int MAX_BYTE_LENGTH  = 334;


/// Default null format string representation
static const char* DEFAULT_NULL_FORMAT = "\\N";

/// Schema map property key for field delimiters
static const char* FIELD_DELIM = "field.delim";
/// Schema map property key for null format
static const char* SERIALIZATION_NULL_FORMAT = "serialization.null.format";


// From: serde/src/gen-java/org/apache/hadoop/hive/serde/Constants.java

static const char* VOID_TYPE_NAME      = "void";
static const char* BOOLEAN_TYPE_NAME   = "boolean";
static const char* TINYINT_TYPE_NAME   = "tinyint";
static const char* SMALLINT_TYPE_NAME  = "smallint";
static const char* INT_TYPE_NAME       = "int";
static const char* BIGINT_TYPE_NAME    = "bigint";
static const char* FLOAT_TYPE_NAME     = "float";
static const char* DOUBLE_TYPE_NAME    = "double";
static const char* STRING_TYPE_NAME    = "string";
static const char* DATE_TYPE_NAME      = "date";
static const char* DATETIME_TYPE_NAME  = "datetime";
static const char* TIMESTAMP_TYPE_NAME = "timestamp";
static const char* LIST_TYPE_NAME      = "array";
static const char* MAP_TYPE_NAME       = "map";
static const char* STRUCT_TYPE_NAME    = "struct";


#endif // __thrift_server_constants_h__
