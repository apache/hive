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
 * @file hiveclienthelper.h
 * @brief Provides some commonly used functions and macros.
 *
 *****************************************************************************/


#ifndef __hive_client_helper_h__
#define __hive_client_helper_h__

#include <iostream>

#include "hiveconstants.h"


// TODO: add architecture specific macro definitions here if needed
#ifdef  ARCH32

#elif defined(ARCH64)

#else

#endif

/*****************************************************************
 * Macro Functions
 *****************************************************************/

/**
 * @brief A macro that converts a string to a signed 64 bit integer.
 *
 * Macro will work for both 32 and 64 bit architectures
 */
#define ATOI64(val)  int64_t(strtoll(val, NULL, 10))

/**
 * @brief A macro that converts a string to an unsigned 64 bit integer.
 *
 * Macro will work for both 32 and 64 bit architectures
 */
#define ATOI64U(val) uint64_t(strtoull(val, NULL, 10))

/**
 * @brief Convert a Macro'ed value to a string.
 *
 * Callers should only call STRINGIFY(x) and
 * should never use XSTRINGIFY(x)
 */
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

/**
 * @brief Finds the number of elements in an array
 */
#define LENGTH(arr) (sizeof(arr)/sizeof(arr[0]))

/**
 * Checks an error condition, and if true:
 * 1. prints the error
 * 2. saves the message to err_buf
 * 3. returns the specified ret_val
 */
#define RETURN_ON_ASSERT(condition, funct_name, error_msg, err_buf, err_buf_len, ret_val) {     \
  if (condition) {                                                                              \
      cerr << funct_name << ": " << error_msg << endl << flush;                                 \
      safe_strncpy(err_buf, error_msg, err_buf_len);                                            \
      return ret_val;                                                                           \
  }                                                                                             \
}

/**
 * Always performs the following:
 * 1. prints the error
 * 2. saves the message to err_buf
 * 3. returns the specified ret_val
 */
#define RETURN_FAILURE(funct_name, error_msg, err_buf, err_buf_len, ret_val) {  \
  RETURN_ON_ASSERT(true, funct_name, error_msg, err_buf, err_buf_len, ret_val); \
}

/*****************************************************************
 * Global Helper Functions
 *****************************************************************/

/**
 * @brief Convert the name of a HiveType to its actual value.
 *
 * Returns the corresponding HiveType enum given the name of a Hive data type.
 * This function is case sensitive.
 * For example: hiveTypeLookup("string") => HIVE_STRING_TYPE
 *
 * @param hive_type_name Name of a HiveType
 * @return The corresponding HiveType
 */
HiveType hiveTypeLookup(const char* hive_type_name);

/**
 * @brief Safe version of strncpy.
 *
 * A version of strncpy that guarantees the existance of '\0' at the end of the supplied buffer
 * to prevent buffer overruns. Instead of returning dest_buffer like strncpy, safe_strncpy
 * returns the number of bytes written to dest_buffer (excluding the null terminator).
 *
 * @param dest_buffer Buffer to write into.
 * @param src_buffer  Buffer to copy from.
 * @param num         The size of the destination buffer in bytes.
 *
 * @return Number of bytes copied into the destination buffer (excluding the null terminator).
 */
size_t safe_strncpy(char* dest_buffer, const char* src_buffer, size_t num);


#endif // __hive_client_helper_h__
