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

#include <assert.h>

#include "HiveColumnDesc.h"
#include "hiveclienthelper.h"
#include "thriftserverconstants.h"


/*************************************************************************************************
 * HiveColumnDesc Class Definition
 ************************************************************************************************/

HiveColumnDesc::HiveColumnDesc() {
  m_hive_type = HIVE_UNKNOWN_TYPE;
  m_is_nullable = false;
  m_is_case_sensitive = false;
  m_max_display_size = 0;
  m_byte_size = 0;
}

HiveColumnDesc::~HiveColumnDesc() {
}

void HiveColumnDesc::initialize(Apache::Hadoop::Hive::FieldSchema& field_schema) {
  m_field_schema = field_schema;
  m_hive_type = hiveTypeLookup(field_schema.type.c_str());
  m_is_nullable = true;
  m_is_case_sensitive = false;
  m_max_display_size = getMaxDisplaySize(m_hive_type);
  m_byte_size = getByteSize(m_hive_type);
}

void HiveColumnDesc::getColumnName(char* buffer, size_t buffer_len) {
  safe_strncpy(buffer, m_field_schema.name.c_str(), buffer_len);
}

void HiveColumnDesc::getColumnType(char* buffer, size_t buffer_len) {
  safe_strncpy(buffer, m_field_schema.type.c_str(), buffer_len);
}

HiveType HiveColumnDesc::getHiveType() {
  return m_hive_type;
}

int HiveColumnDesc::getIsNullable() {
  return m_is_nullable ? 1 : 0;
}

int HiveColumnDesc::getIsCaseSensitive() {
  return m_is_case_sensitive ? 1 : 0;
}

size_t HiveColumnDesc::getMaxDisplaySize() {
  return m_max_display_size;
}

size_t HiveColumnDesc::getFieldByteSize() {
  return m_byte_size;
}

size_t HiveColumnDesc::getMaxDisplaySize(HiveType type) {
  /* These are more or less arbitrarily determined values and may be changed if it causes any problems */
  switch (type)
  {
  case HIVE_VOID_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_BOOLEAN_TYPE:
    return 1;

  case HIVE_TINYINT_TYPE:
    return 4;

  case HIVE_SMALLINT_TYPE:
    return 6;

  case HIVE_INT_TYPE:
    return 11;

  case HIVE_BIGINT_TYPE:
    return 20;

  case HIVE_FLOAT_TYPE:
    return 16;

  case HIVE_DOUBLE_TYPE:
    return 24;

  case HIVE_STRING_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_DATE_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_DATETIME_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_TIMESTAMP_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_LIST_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_MAP_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_STRUCT_TYPE:
    return MAX_DISPLAY_SIZE;

  case HIVE_UNKNOWN_TYPE:
    return MAX_DISPLAY_SIZE;

  default:
    return MAX_DISPLAY_SIZE;
  }
}

size_t HiveColumnDesc::getByteSize(HiveType type) {
  /* These are more or less arbitrarily determined values and may be changed if it causes any problems */
  switch (type)
  {
  case HIVE_VOID_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_BOOLEAN_TYPE:
    return 1;

  case HIVE_TINYINT_TYPE:
    return 1;

  case HIVE_SMALLINT_TYPE:
    return 2;

  case HIVE_INT_TYPE:
    return 4;

  case HIVE_BIGINT_TYPE:
    return 8;

  case HIVE_FLOAT_TYPE:
    return 4;

  case HIVE_DOUBLE_TYPE:
    return 8;

  case HIVE_STRING_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_DATE_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_DATETIME_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_TIMESTAMP_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_LIST_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_MAP_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_STRUCT_TYPE:
    return MAX_BYTE_LENGTH;

  case HIVE_UNKNOWN_TYPE:
    return MAX_BYTE_LENGTH;

  default:
    return MAX_BYTE_LENGTH;
  }
}
