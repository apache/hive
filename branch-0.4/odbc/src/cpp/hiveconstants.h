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
 * @file hiveconstants.h
 * @brief Provides constants necessary for library caller interaction with Hive Client
 *
 *****************************************************************************/


#ifndef __hive_constants_h__
#define __hive_constants_h__

/// Maximum length of a Hive Client error message
static const int MAX_HIVE_ERR_MSG_LEN = 128;
/// Maximum length of a column name
static const int MAX_COLUMN_NAME_LEN  = 64;
/// Maximum length of a column type name
static const int MAX_COLUMN_TYPE_LEN  = 64;


/* Default connection parameters */
/// Default Hive database name
static const char* DEFAULT_DATABASE = "default";
/// Default Hive Server host
static const char* DEFAULT_HOST     = "localhost";
/// Default Hive Server port
static const char* DEFAULT_PORT     = "10000";
/// Default connection socket type
static const char* DEFAULT_FRAMED   = "0";

/**
 * Enumeration of known Hive data types
 */
enum HiveType
{
  HIVE_VOID_TYPE,
  HIVE_BOOLEAN_TYPE,
  HIVE_TINYINT_TYPE,
  HIVE_SMALLINT_TYPE,
  HIVE_INT_TYPE,
  HIVE_BIGINT_TYPE,
  HIVE_FLOAT_TYPE,
  HIVE_DOUBLE_TYPE,
  HIVE_STRING_TYPE,
  HIVE_DATE_TYPE,
  HIVE_DATETIME_TYPE,
  HIVE_TIMESTAMP_TYPE,
  HIVE_LIST_TYPE,
  HIVE_MAP_TYPE,
  HIVE_STRUCT_TYPE,
  HIVE_UNKNOWN_TYPE
};

/**
 * Enumeration of Hive return values
 */
enum HiveReturn
{
  HIVE_SUCCESS,
  HIVE_ERROR,
  HIVE_NO_MORE_DATA,
  HIVE_SUCCESS_WITH_MORE_DATA,
  HIVE_STILL_EXECUTING
};

#endif // __hive_constants_h__
