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
 * @file HiveRowSet.h
 * @brief Provides the HiveRowSet interface and subclasses.
 *
 *****************************************************************************/


#ifndef __hive_rowset_h__
#define __hive_rowset_h__

#include <iostream>

#include "hive_metastore_types.h"

#include "hiveconstants.h"
#include "thriftserverconstants.h"

using namespace std;


/*************************************************************************************************
 * Base HiveRowSet Class Abstract Declaration
 ************************************************************************************************/

/**
 * @brief HiveRowSet interface definition.
 *
 * Abstract base class for Hive rowsets. Provides the logic to extract fields as various
 * data types, allowing subclasses to focus on the storage and field parsing of the data.
 */
class HiveRowSet {
  public:
    /// Cannot be called directly, but should be called automatically from subclass constructors
    HiveRowSet();
    virtual ~HiveRowSet();
    void reset(); ///< Not overrideable, implement specialized_reset() instead
    HiveReturn getFieldDataLen(size_t column_idx, size_t* col_len, char* err_buf, size_t err_buf_len);
    HiveReturn getFieldAsCString(size_t column_idx, char* buffer, size_t buffer_len,
                                 size_t* data_byte_size, int* is_null_value, char* err_buf,
                                 size_t err_buf_len);
    HiveReturn getFieldAsDouble(size_t column_idx, double* buffer, int* is_null_value, char* err_buf,
                                size_t err_buf_len);
    HiveReturn getFieldAsInt(size_t column_idx, int* buffer, int* is_null_value, char* err_buf,
                             size_t err_buf_len);
    HiveReturn getFieldAsLong(size_t column_idx, long* buffer, int* is_null_value, char* err_buf,
                              size_t err_buf_len);
    HiveReturn getFieldAsULong(size_t column_idx, unsigned long* buffer, int* is_null_value,
                               char* err_buf, size_t err_buf_len);
    HiveReturn getFieldAsI64(size_t column_idx, int64_t* buffer, int* is_null_value, char* err_buf,
                             size_t err_buf_len);
    HiveReturn getFieldAsI64U(size_t column_idx, uint64_t* buffer, int* is_null_value, char* err_buf,
                              size_t err_buf_len);

  protected:
    /// Forces all data retrieved to be no more than MAX_BYTE_LENGTH
    char m_field_buffer[MAX_BYTE_LENGTH + 1];

    /**
     * @brief Initializes m_field_buffer with the field indicated by m_last_column_fetched.
     *
     * Not overrideable, used by subclasses to synchronize m_field_buffer and m_last_column_fetched;
     * should be called by every subclass immediately after initialization. This is necessary at
     * the beginning to make sure that m_field_buffer will always have the field data indicated
     * by m_last_column_fetched.
     */
    void initFieldBuffer();

  private:
    bool m_is_completely_read;
    size_t m_last_column_fetched;
    /// Number of bytes read out of m_last_column_fetched since the last consecutive call
    size_t m_bytes_read;

    virtual void specialized_reset() =0; ///< Called by HiveRowSet::reset()
    virtual size_t getColumnCount() =0; ///< Only used within this class hierarchy for error checking
    virtual const char* getNullFormat() =0; ///< Should return a pointer to a locally stored C string
    virtual size_t getFieldLen(size_t column_idx) =0;
    /// Should copy the field data as a C string to m_field_buffer
    virtual void extractField(size_t column_idx) =0;
};


/*************************************************************************************************
 * HiveSerializedRowSet Subclass Declaration
 ************************************************************************************************/

/**
 * @brief Container class for a fetched row from a HiveResultSet where each row is a serialized string.
 *
 * Container class for a fetched row from a HiveResultSet where each row is a serialized string.
 * - HiveSerializedRowSet is completely dependent on associated HiveResultSet and should not
 *     be used independently; must always remain bound to associated HiveResultSet.
 * - Assumes the associated HiveResultSet manages the memory of all weak pointer members
 * - Constructed within the HiveResultSet by calling DBFetch
 * - All errors messages will be written to err_buf if err_buf and err_buf_len are provided
 */
class HiveSerializedRowSet: public HiveRowSet {
  public:
    HiveSerializedRowSet();
    virtual ~HiveSerializedRowSet();
    void initialize(Apache::Hadoop::Hive::Schema& schema, string& serialized_row);

  private:
    string* m_row_weak_ptr; ///< Weak pointer to the row string associated with m_field_offsets
    vector<size_t> m_field_offsets; ///< Indexes into the serialized row string of column starting points
    string* m_null_format_weak_ptr; ///< Weak pointer to NULL format representation for this row

    void specialized_reset();
    void initializeOffsets(Apache::Hadoop::Hive::Schema& schema, string& serialized_row);
    size_t getColumnCount();
    const char* getNullFormat();
    size_t getFieldLen(size_t column_idx);
    void extractField(size_t column_idx);
};


/*************************************************************************************************
 * HiveStringVectorRowSet Subclass Declaration
 ************************************************************************************************/

/**
 * @brief Container class for a fetched row from a HiveResultSet where each row is vector of string fields.
 *
 * Container class for a fetched row from a HiveResultSet where each row is vector of string fields
 * - HiveStringVectorRowSet is completely dependent on associated HiveResultSet and should not
 *     be used independently; must always remain bound to associated HiveResultSet.
 * - Assumes the associated HiveResultSet manages the memory of all weak pointer members
 * - Constructed within the HiveResultSet by calling DBFetch
 * - All errors messages will be written to err_buf if err_buf and err_buf_len are provided
 */
class HiveStringVectorRowSet: public HiveRowSet {
  public:
    HiveStringVectorRowSet();
    virtual ~HiveStringVectorRowSet();
    void initialize(Apache::Hadoop::Hive::Schema& schema, vector<string>* fields);

  private:
    vector<string>* m_fields_weak_ptr; ///< Weak pointer to a vector of fields represented as strings
    string* m_null_format_weak_ptr; ///< Weak pointer to NULL format representation for this row

    void specialized_reset();
    size_t getColumnCount();
    const char* getNullFormat();
    size_t getFieldLen(size_t column_idx);
    void extractField(size_t column_idx);
};


#endif // __hive_rowset_h__
