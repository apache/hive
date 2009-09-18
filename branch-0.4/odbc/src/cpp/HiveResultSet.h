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
 * @file HiveResultSet.h
 * @brief Provides the HiveResultSet interface definition and subclasses.
 *
 *****************************************************************************/


#ifndef __hive_resultset_h__
#define __hive_resultset_h__

#include <iostream>

#include "hive_metastore_types.h"

#include "hiveconstants.h"
#include "HiveConnection.h"
#include "HiveRowSet.h"
#include "HiveColumnDesc.h"

using namespace std;


/*************************************************************************************************
 * Base HiveResultSet Class Abstract Declaration
 ************************************************************************************************/

/**
 * @brief HiveResultSet interface definition.
 *
 * Abstract base class for Hive resultsets. Does not provide any additional logic for
 * subclasses, but defines the interface expected for all HiveResultSets.
 * All subclasses extending HiveResultSet need to at least implement the below methods.
 */
class HiveResultSet {
  public:
    virtual ~HiveResultSet() {} ///< The constructor should be defined independently by each subclass
    virtual HiveReturn fetchNext(char* err_buf, size_t err_buf_len) =0;
    virtual HiveReturn hasResults(int* results, char* err_buf, size_t err_buf_len) =0;
    virtual HiveReturn getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len) =0;
    virtual HiveReturn createColumnDesc(size_t column_idx, HiveColumnDesc** column_desc_ptr,
                                        char* err_buf, size_t err_buf_len) =0;
    /// The rowset will ONLY be valid after fetchNext has been called at least once
    virtual HiveRowSet& getRowSet() =0;
};


/*************************************************************************************************
 * HiveQueryResultSet Subclass Declaration
 ************************************************************************************************/

/**
 * @brief A container for the resultsets of Hive queries.
 *
 * Container class for a query result set (the result of a DBExecute).
 * This class was only meant to be created by DBExecute and destroyed by DBCloseResultSet.
 * Implements a lazy row/field extraction approach.
 * A single instance should only belong to no more than one thread.
 * All errors messages will be written to err_buf if err_buf and err_buf_len are provided.
 */
class HiveQueryResultSet: public HiveResultSet {
  public:
    HiveQueryResultSet(int max_buf_rows);
    virtual ~HiveQueryResultSet();
    HiveReturn initialize(HiveConnection* connection, char* err_buf, size_t err_buf_len);
    HiveReturn fetchNext(char* err_buf, size_t err_buf_len);
    HiveReturn hasResults(int* results, char* err_buf, size_t err_buf_len);
    HiveReturn getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len);
    HiveReturn createColumnDesc(size_t column_idx, HiveColumnDesc** column_desc_ptr, char* err_buf,
                                size_t err_buf_len);
    HiveRowSet& getRowSet();

  private:
    HiveConnection* m_connection; ///< Hive connection handle
    HiveSerializedRowSet m_serial_rowset; ///< Rowset associated with the current fetched row (if any)
    int m_max_buffered_rows; ///< Max number of rows to buffer in client memory
    int m_fetch_idx; ///< Last row fetched by the client
    bool m_has_results; ///< Indicates that at least one result row has been successfully fetched
    bool m_fetch_attempted; ///< Indicates that a Hive server fetch call has successfully executed
    vector<string> m_result_set_data; ///< Vector of serialized rows
    Apache::Hadoop::Hive::Schema m_schema; ///< Schema of the result table

    HiveReturn initializeSchema(char* err_buf, size_t err_buf_len);
    HiveReturn fetchNewResults(char* err_buf, size_t err_buf_len);
};


/*************************************************************************************************
 * HiveTablesResultSet Subclass Declaration
 ************************************************************************************************/

/**
 * @brief A container for resultsets describing the database table catalog.
 *
 * Container class for a pre-scripted table list resultset (the result of a DBTables).
 * This class was only meant to be created by DBTables and destroyed by DBCloseResultSet.
 * All error messages will be written to err_buf if err_buf and err_buf_len are provided.
 * This is a very rudimentary implementation b/c not much table info is available.
 */
class HiveTablesResultSet: public HiveResultSet {
  public:
    HiveTablesResultSet();
    virtual ~HiveTablesResultSet();
    HiveReturn initialize(HiveConnection* connection, const char* tbl_search_pattern, char* err_buf,
                          size_t err_buf_len);
    HiveReturn fetchNext(char* err_buf, size_t err_buf_len);
    HiveReturn hasResults(int* results, char* err_buf, size_t err_buf_len);
    HiveReturn getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len);
    HiveReturn createColumnDesc(size_t column_idx, HiveColumnDesc** column_desc_ptr, char* err_buf,
                                size_t err_buf_len);
    HiveRowSet& getRowSet();

  private:
    int m_fetch_idx; ///< Last row fetched by the client
    /// OK to use vector<string> b/c greatly simplifies work and class not used often
    vector<string> m_curr_row_data; ///< Vector with row data corresponding to row at m_fetch_idx
    /// Rowset associated with the current fetched row (if any)
    HiveStringVectorRowSet m_vecstring_rowset;
    vector<string> m_tables; ///< Vector of table names
    Apache::Hadoop::Hive::Schema m_schema; ///< Schema of the result table

    HiveReturn initializeSchema(char* err_buf, size_t err_buf_len);
    HiveReturn constructCurrentRow(char* err_buf, size_t err_buf_len);
};


/*************************************************************************************************
 * HiveColumnsResultSet Subclass Declaration
 ************************************************************************************************/

/**
 * @brief A container for resultsets describing the columns of table(s).
 *
 * Container class for a pre-scripted column info resultset (the result of a DBColumns).
 * This class was only meant to be created by DBColumns and destroyed by DBCloseResultSet.
 * All error messages will be written to err_buf if err_buf and err_buf_len are provided.
 * This is a very rudimentary implementation b/c not much column info is available.
 */
class HiveColumnsResultSet: public HiveResultSet {
  public:
    /// Constructor requires a Hive-to-SQL type convert function pointer as an argument
    HiveColumnsResultSet(int(*fpHiveToSQLType)(HiveType));
    virtual ~HiveColumnsResultSet();
    HiveReturn initialize(HiveConnection* connection, const char* tbl_search_pattern,
                          const char* col_search_pattern, char* err_buf, size_t err_buf_len);
    HiveReturn fetchNext(char* err_buf, size_t err_buf_len);
    HiveReturn hasResults(int* results, char* err_buf, size_t err_buf_len);
    HiveReturn getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len);
    HiveReturn createColumnDesc(size_t column_idx, HiveColumnDesc** column_desc_ptr, char* err_buf,
                                size_t err_buf_len);
    HiveRowSet& getRowSet();

  private:
    HiveConnection* m_connection; ///< Hive connection handle
    int (*m_fpHiveToSQLType)(HiveType); ///< Pointer to HiveType to SQLType convert function
    int m_tbl_fetch_idx; ///< Last table fetched
    int m_col_fetch_idx; ///< Last column fetched
    vector<string> m_tables; ///< Vector of table names
    vector<Apache::Hadoop::Hive::FieldSchema> m_columns; ///< Vector of column field schemas
    /// OK to use vector<string> b/c greatly simplifies work and class not used often
    vector<string> m_curr_row_data; ///< Vector with constructed row data
    /// Rowset associated with the current constructed row (if any)
    HiveStringVectorRowSet m_vecstring_rowset;
    Apache::Hadoop::Hive::Schema m_schema; ///< Schema of the result table

    HiveReturn getNextTableFields(char* err_buf, size_t err_buf_len);
    HiveReturn initializeSchema(char* err_buf, size_t err_buf_len);
    HiveReturn constructCurrentRow(char* err_buf, size_t err_buf_len);
};


#endif // __hive_resultset_h__
