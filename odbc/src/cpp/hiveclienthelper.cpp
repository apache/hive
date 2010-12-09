/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <assert.h>
#include <string.h>

#include "hiveclienthelper.h"
#include "thriftserverconstants.h"

using namespace std;

/*****************************************************************
 * File Scope Variables (used only within this file)
 *****************************************************************/

/*
 * g_hive_type_table: An array of HiveTypePairs used to define the mapping
 *       between the name of a HiveType and its value. This array is only
 *       used by the hiveTypeLookup function.
 */
struct HiveTypePair {
  const char* type_name;
  const HiveType type_value;
};

static const HiveTypePair g_hive_type_table[] = {
        {VOID_TYPE_NAME,      HIVE_VOID_TYPE},
        {BOOLEAN_TYPE_NAME,   HIVE_BOOLEAN_TYPE},
        {TINYINT_TYPE_NAME,   HIVE_TINYINT_TYPE},
        {SMALLINT_TYPE_NAME,  HIVE_SMALLINT_TYPE},
        {INT_TYPE_NAME,       HIVE_INT_TYPE},
        {BIGINT_TYPE_NAME,    HIVE_BIGINT_TYPE},
        {FLOAT_TYPE_NAME,     HIVE_FLOAT_TYPE},
        {DOUBLE_TYPE_NAME,    HIVE_DOUBLE_TYPE},
        {STRING_TYPE_NAME,    HIVE_STRING_TYPE},
        {DATE_TYPE_NAME,      HIVE_DATE_TYPE},
        {DATETIME_TYPE_NAME,  HIVE_DATETIME_TYPE},
        {TIMESTAMP_TYPE_NAME, HIVE_TIMESTAMP_TYPE},
        {LIST_TYPE_NAME,      HIVE_LIST_TYPE},
        {MAP_TYPE_NAME,       HIVE_MAP_TYPE},
        {STRUCT_TYPE_NAME,    HIVE_STRUCT_TYPE}
};

/*****************************************************************
 * Global Helper Functions
 *****************************************************************/

HiveType hiveTypeLookup(const char* hive_type_name) {
  assert(hive_type_name != NULL);
  for (unsigned int idx = 0; idx < LENGTH(g_hive_type_table); idx++) {
    if (strcmp(hive_type_name, g_hive_type_table[idx].type_name) == 0) {
      return g_hive_type_table[idx].type_value;
    }
  }
  return HIVE_UNKNOWN_TYPE;
}

size_t safe_strncpy(char* dest_buffer, const char* src_buffer, size_t num) {
  /* Make sure arguments are valid */
  if (num == 0 || dest_buffer == NULL || src_buffer == NULL)
    return 0;

  size_t len = min(num - 1, strlen(src_buffer));
  strncpy(dest_buffer, src_buffer, len); /* Copy only as much as needed */
  dest_buffer[len] = '\0'; /* NULL terminate in case strncpy copies exactly num-1 characters */

  /* Returns the number of bytes copied into the destination buffer (excluding the null terminator)*/
  return len;
}

