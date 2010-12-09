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
#include <string.h>

#include "HiveRowSet.h"
#include "hiveclienthelper.h"


/*************************************************************************************************
 * Base HiveRowSet Class Logic
 ************************************************************************************************/

HiveRowSet::HiveRowSet() {
  m_is_completely_read = false;
  m_bytes_read = 0;
  m_last_column_fetched = 0;
  m_field_buffer[0] = '\0';
}

HiveRowSet::~HiveRowSet() {
}

void HiveRowSet::reset() {
  m_is_completely_read = false;
  m_bytes_read = 0;
  m_last_column_fetched = 0;
  m_field_buffer[0] = '\0';
  /* Non Virtual Calls Pure Virtual Idiom */
  specialized_reset(); /* Call the specialized subclass reset method */
}

void HiveRowSet::initFieldBuffer() {
  /* m_field_buffer should always correspond to the field indicated by m_last_column_fetched*/
  extractField(m_last_column_fetched);
}

HiveReturn HiveRowSet::getFieldDataLen(size_t column_idx, size_t* col_len, char* err_buf,
                                       size_t err_buf_len) {
  RETURN_ON_ASSERT(col_len == NULL, __FUNCTION__,
                   "Pointer to col_len (output) cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);
  *col_len = getFieldLen(column_idx);
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsCString(size_t column_idx, char* buffer, size_t buffer_len,
                                         size_t* data_byte_size, int* is_null_value, char* err_buf,
                                         size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(buffer_len == 0, __FUNCTION__,
                   "Output buffer cannot have a size of zero.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This field has already been completely fetched by a previous call*/
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    /* This value must be NULL */
    *is_null_value = 1;
    if (data_byte_size != NULL) {
      *data_byte_size = 0;
    }
    buffer[0] = '\0';
  } else {
    /* This value has been determined not to be NULL */
    *is_null_value = 0;
    size_t data_total_len = getFieldLen(column_idx);
    /* Cannot read more data then the total number of bytes available */
    assert(data_total_len >= m_bytes_read);
    size_t bytes_remaining = data_total_len - m_bytes_read; // Excludes null char
    if (data_byte_size != NULL) {
      /* Save the number of remaining characters to return before this fetch */
      *data_byte_size = bytes_remaining;
    }
    /* Move pointer to the read location */
    const char* src_str_ptr = m_field_buffer + m_bytes_read;
    /* The total number of bytes to read (+1 null terminator) should be no more than the
     * size of the field buffer */
    assert(m_bytes_read + bytes_remaining + 1 <= sizeof(m_field_buffer));
    /* Copy as many characters as possible from the read location */
    size_t bytes_copied = safe_strncpy(buffer, src_str_ptr, min(buffer_len, bytes_remaining + 1)); // +1 for null terminator
    /* bytes_copied does not count the null terminator */
    m_bytes_read += bytes_copied;
    if (m_bytes_read < data_total_len) {
      return HIVE_SUCCESS_WITH_MORE_DATA; /* Data truncated; more data to return */
    }
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS; /* All data successfully read */
}

HiveReturn HiveRowSet::getFieldAsDouble(size_t column_idx, double* buffer, int* is_null_value,
                                        char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    /* Reset if this column was not fetched on the last attempt */
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the nullformat spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0.0;
  } else {
    *is_null_value = 0;
    *buffer = atof(m_field_buffer);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsInt(size_t column_idx, int* buffer, int* is_null_value,
                                     char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    *buffer = atoi(m_field_buffer);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsLong(size_t column_idx, long* buffer, int* is_null_value,
                                      char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    *buffer = atol(m_field_buffer);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsULong(size_t column_idx, unsigned long* buffer,
                                       int* is_null_value, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    *buffer = strtoul(m_field_buffer, NULL, 10);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsI64(size_t column_idx, int64_t* buffer, int* is_null_value,
                                     char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    *buffer = ATOI64(m_field_buffer);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

HiveReturn HiveRowSet::getFieldAsI64U(size_t column_idx, uint64_t* buffer, int* is_null_value,
                                      char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(buffer == NULL, __FUNCTION__,
                   "Column data output buffer cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(is_null_value == NULL, __FUNCTION__,
                   "Column data is_null_value (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(getColumnCount() == 0, __FUNCTION__,
                   "Rowset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= getColumnCount(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  if (m_last_column_fetched != column_idx) {
    extractField(column_idx);
    m_bytes_read = 0; /* Reset the read offset if different from the last column fetched */
    m_last_column_fetched = column_idx;
    m_is_completely_read = false;
  }
  if (m_is_completely_read) {
    return HIVE_NO_MORE_DATA; /* This column has already been completely fetched */
  }
  /* If the column data is the same as the null format spec... */
  if (strcmp(getNullFormat(), m_field_buffer) == 0) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    *buffer = ATOI64U(m_field_buffer);
  }
  m_is_completely_read = true;
  return HIVE_SUCCESS;
}

/*************************************************************************************************
 * HiveSerializedRowSet Subclass Definition
 ************************************************************************************************/

HiveSerializedRowSet::HiveSerializedRowSet() {
  m_row_weak_ptr = NULL;
  m_null_format_weak_ptr = NULL;
}

HiveSerializedRowSet::~HiveSerializedRowSet() {
  /* Nothing to deallocate */
}

void HiveSerializedRowSet::initialize(Apache::Hadoop::Hive::Schema& schema, string& serialized_row) {
  m_row_weak_ptr = &serialized_row;
  /* Allocate sufficient space to prevent further resizing */
  m_field_offsets.reserve(schema.fieldSchemas.size());
  initializeOffsets(schema, serialized_row); // Initialize m_field_offsets
  assert(m_field_offsets.size() == schema.fieldSchemas.size());
  assert(schema.properties[SERIALIZATION_NULL_FORMAT].length() > 0);
  m_null_format_weak_ptr = &(schema.properties[SERIALIZATION_NULL_FORMAT]);
  /* Synchronize m_field_buffer and m_last_column_fetched now that extractField() works */
  initFieldBuffer();
}

/* This method should never be called outside of the inherited HiveRowSet::reset() */
void HiveSerializedRowSet::specialized_reset() {
  m_row_weak_ptr = NULL;
  m_field_offsets.clear();
  m_null_format_weak_ptr = NULL;
}

void HiveSerializedRowSet::initializeOffsets(Apache::Hadoop::Hive::Schema& schema, string& serialized_row) {
  m_field_offsets.push_back(0); // There will always be at least one column
  // Keep a temporary field_delim reference so we don't have to keep using the map
  string& field_delim(schema.properties[FIELD_DELIM]);
  assert(field_delim.length() > 0);

  // Assumes that field delimiters will only be one character
  size_t idx = serialized_row.find_first_of(field_delim);
  while (idx != string::npos) {
    // Set the field offset to the start of the following field
    m_field_offsets.push_back(idx + 1);
    idx = serialized_row.find_first_of(field_delim, idx + 1);
  }
}

size_t HiveSerializedRowSet::getColumnCount() {
  return m_field_offsets.size();
}

const char* HiveSerializedRowSet::getNullFormat() {
  assert(m_null_format_weak_ptr != NULL);
  return m_null_format_weak_ptr->c_str();
}

size_t HiveSerializedRowSet::getFieldLen(size_t column_idx) {
  assert(column_idx < getColumnCount());
  assert(m_row_weak_ptr != NULL);
  size_t len;
  // If this is the last column...
  if (column_idx == getColumnCount() - 1) {
    assert(m_row_weak_ptr->length() >= m_field_offsets[column_idx]);
    len = m_row_weak_ptr->length() - m_field_offsets[column_idx];
  } else {
    assert(m_field_offsets[column_idx + 1] > m_field_offsets[column_idx]);
    len = m_field_offsets[column_idx + 1] - m_field_offsets[column_idx] - 1;
  }
  /* Enforce the constraint that no data exceed MAX_BYTE_LENGTH */
  len = min(len, (size_t) MAX_BYTE_LENGTH);
  return len;
}

void HiveSerializedRowSet::extractField(size_t column_idx) {
  assert(column_idx < getColumnCount());
  assert(m_row_weak_ptr != NULL);
  /* The field buffer should always be large enough to hold the field */
  assert(getFieldLen(column_idx) < sizeof(m_field_buffer));
  /* Just safety precaution to prevent buffer overflow */
  /* Reduce buffer size by one to save space for null terminator */
  size_t extract_len = min(getFieldLen(column_idx), sizeof(m_field_buffer) - 1);
  size_t copied = m_row_weak_ptr->copy(m_field_buffer, extract_len, m_field_offsets[column_idx]);
  assert(copied == extract_len);
  /* Make sure the buffer is null terminated */
  m_field_buffer[extract_len] = '\0';
}

/*************************************************************************************************
 * HiveStringVectorRowSet Subclass Definition
 ************************************************************************************************/

HiveStringVectorRowSet::HiveStringVectorRowSet() {
  m_fields_weak_ptr = NULL;
  m_null_format_weak_ptr = NULL;
}

HiveStringVectorRowSet::~HiveStringVectorRowSet() {
  /* Nothing to deallocate */
}

void HiveStringVectorRowSet::initialize(Apache::Hadoop::Hive::Schema& schema, vector<string>* fields) {
  assert(fields != NULL);
  m_fields_weak_ptr = fields;
  assert(schema.properties[SERIALIZATION_NULL_FORMAT].length() > 0);
  m_null_format_weak_ptr = &(schema.properties[SERIALIZATION_NULL_FORMAT]);
  /* Synchronize m_field_buffer and m_last_column_fetched now that extractField() works */
  initFieldBuffer();
}

/* This method should never be called outside of the inherited HiveRowSet::reset() */
void HiveStringVectorRowSet::specialized_reset() {
  m_fields_weak_ptr = NULL;
  m_null_format_weak_ptr = NULL;
}

size_t HiveStringVectorRowSet::getColumnCount() {
  assert(m_fields_weak_ptr != NULL);
  return m_fields_weak_ptr->size();
}

const char* HiveStringVectorRowSet::getNullFormat() {
  assert(m_null_format_weak_ptr != NULL);
  return m_null_format_weak_ptr->c_str();
}

size_t HiveStringVectorRowSet::getFieldLen(size_t column_idx) {
  assert(column_idx < getColumnCount());
  assert(m_fields_weak_ptr != NULL);
  size_t len = m_fields_weak_ptr->at(column_idx).length();
  /* Enforce the constraint that no data exceed MAX_BYTE_LENGTH */
  len = min(len, (size_t) MAX_BYTE_LENGTH);
  return len;
}

void HiveStringVectorRowSet::extractField(size_t column_idx) {
  assert(column_idx < getColumnCount());
  assert(m_fields_weak_ptr != NULL);
  safe_strncpy(m_field_buffer, m_fields_weak_ptr->at(column_idx).c_str(), sizeof(m_field_buffer));
}


