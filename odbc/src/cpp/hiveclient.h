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
 * @file hiveclient.h
 * @brief Hive client library interface.
 *
 * This header file defines the Hive client library interface (C compatible).
 * All interactions with Hive client should be directed through the following
 * declared functions.
 *
 * Here is an example of a typical order with which to call these functions:
 *
 * DBOpenConnection
 * ..-> DBExecute or DBTables or DBColumns
 * ..-> DBCreateColumnDesc
 * ....-> DBGetColumnName
 * ....-> DBGetColumnType
 * ....-> DBGetHiveType
 * ....-> DBGetIsNullable
 * ....-> DBGetIsCaseSensitive
 * ....-> DBGetMaxDisplaySize
 * ....-> DBGetFieldByteSize
 * ....-> DBCloseColumnDesc
 * ..-> DBHasResults
 * ..-> DBGetColumnCount
 * ..-> DBFetch
 * ....-> DBGetFieldDataLen
 * ....-> DBGetFieldAsCString
 * ....-> DBGetFieldAsDouble
 * ....-> DBGetFieldAsInt
 * ....-> DBGetFieldAsLong
 * ....-> DBGetFieldAsULong
 * ....-> DBGetFieldAsI64
 * ....-> DBGetFieldAsI64U
 * ..-> DBCloseResultSet
 * ..-> DBCloseConnection
 *
 *****************************************************************************/


#ifndef __hive_client_h__
#define __hive_client_h__

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <stdint.h>

#include "hiveconstants.h"


/******************************************************************************
 * Hive Client C++ Class Placeholders
 *****************************************************************************/

typedef enum HiveReturn HiveReturn;
typedef enum HiveType HiveType;
typedef struct HiveConnection HiveConnection;
typedef struct HiveResultSet HiveResultSet;
typedef struct HiveColumnDesc HiveColumnDesc;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus


/******************************************************************************
 * Global Hive Client Functions (usable as C callback functions)
 *****************************************************************************/

/**
 * @brief Connect to a Hive database.
 *
 * Connects to a Hive database on the specified Hive server. The caller takes
 * ownership of the returned HiveConnection and is responsible for deallocating
 * the memory and connection by calling DBCloseConnection.
 *
 * @see DBCloseConnection()
 *
 * @param database    Name of the Hive database on the Hive server to which to connect.
 * @param host        Host address of the Hive server.
 * @param port        Host port of the Hive server.
 * @param framed      Set as 1 to use a framed socket, or 0 to use a buffered socket.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return A HiveConnection object representing the established database connection,
 *         or NULL if an error occurred. Error messages will be stored in err_buf.
 */
HiveConnection* DBOpenConnection(const char* database, const char* host, int port, int framed,
                 char* err_buf, size_t err_buf_len);

/**
 * @brief Disconnects from a Hive database.
 *
 * Disconnects from a Hive database and destroys the supplied HiveConnection object.
 * This function should eventually be called for every HiveConnection created by
 * DBOpenConnection.
 *
 * @see DBOpenConnection()
 *
 * @param connection  A HiveConnection object associated a database connection.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBCloseConnection(HiveConnection* connection, char* err_buf, size_t err_buf_len);

/**
 * @brief Execute a query.
 *
 * Executes a query on a Hive connection and associates a HiveResultSet with the result.
 * Caller takes ownership of returned HiveResultSet and is responsible for deallocating
 * the object by calling DBCloseResultSet.
 *
 * @see DBCloseResultSet()
 *
 * @param connection    A HiveConnection object associated a database connection.
 * @param query         The Hive query string to be executed.
 * @param resultset_ptr A pointer to a HiveResultSet pointer which will be associated with the
 *                      result, or NULL if the result is not needed.
 * @param max_buf_rows  Maximum number of rows to buffer in the new HiveResultSet for the query
 *                      results
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBExecute(HiveConnection* connection, const char* query, HiveResultSet** resultset_ptr,
           int max_buf_rows, char* err_buf, size_t err_buf_len);

/**
 * @brief Query for database tables.
 *
 * Gets a resultset containing the set of tables in the database matching a regex pattern.
 * Caller takes ownership of returned HiveResultSet and is responsible for deallocating
 * the object by calling DBCloseResultSet.
 *
 * @see DBCloseResultSet()
 *
 * @param connection         A HiveConnection object associated a database connection.
 * @param tbl_search_pattern A regex pattern used to match with table names (use '*' to match all).
 * @param resultset_ptr      A pointer to a HiveResultSet pointer which will be associated with the
 *                           result.
 * @param err_buf            Buffer to receive an error message if HIVE_ERROR is returned.
 *                           NULL can be used if the caller does not care about the error message.
 * @param err_buf_len        Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBTables(HiveConnection* connection, const char* tbl_search_pattern,
          HiveResultSet** resultset_ptr, char* err_buf, size_t err_buf_len);

/**
 * @brief Query for columns in table(s).
 *
 * Gets a resultset containing the set of columns in the database matching a regex pattern
 * from a set of tables matching another regex pattern. Caller takes ownership of returned
 * HiveResultSet and is responsible for deallocating the object by calling DBCloseResultSet.
 *
 * @see DBCloseResultSet()
 *
 * @param connection         A HiveConnection object associated with a database connection.
 * @param fpHiveToSQLType    Pointer to a function that takes a HiveType argument and returns a
 *                           corresponding int value (possibly a SQL type).
 * @param tbl_search_pattern A regex pattern used to match with table names (use '*' to match all).
 * @param col_search_pattern A regex pattern used to match with column names (use '*' to match all).
 * @param resultset_ptr      A pointer to a HiveResultSet pointer which will be associated with the
 *                           result.
 * @param err_buf            Buffer to receive an error message if HIVE_ERROR is returned.
 *                           NULL can be used if the caller does not care about the error message.
 * @param err_buf_len        Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBColumns(HiveConnection* connection, int (*fpHiveToSQLType)(HiveType),
           const char* tbl_search_pattern, const char* col_search_pattern,
           HiveResultSet** resultset_ptr, char* err_buf, size_t err_buf_len);

/**
 * @brief Destroys any specified HiveResultSet object.
 *
 * Destroys any specified HiveResultSet object. The HiveResultSet may have been
 * created by a number of other functions.
 *
 * @param resultset   A HiveResultSet object to be removed from memory.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBCloseResultSet(HiveResultSet* resultset, char* err_buf, size_t err_buf_len);

/**
 * @brief Fetches the next unfetched row in a HiveResultSet.
 *
 * Fetches the next unfetched row in a HiveResultSet. The fetched row will be stored
 * internally within the resultset and may be accessed through other DB functions.
 *
 * @param resultset   A HiveResultSet from which to fetch rows.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful,
 *         HIVE_ERROR if an error occurred,
 *         HIVE_NO_MORE_DATA if there are no more rows to fetch.
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBFetch(HiveResultSet* resultset, char* err_buf, size_t err_buf_len);

/**
 * @brief Check for results.
 *
 * Determines whether the HiveResultSet has ever had any result rows.
 *
 * @param resultset   A HiveResultSet from which to check for results.
 * @param has_results Pointer to an int which will be set to 1 if results, 0 if no results.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBHasResults(HiveResultSet* resultset, int* has_results, char* err_buf,
            size_t err_buf_len);

/**
 * @brief Determines the number of columns in the HiveResultSet.
 *
 * @param resultset   A HiveResultSet from which to retrieve the column count.
 * @param col_count   Pointer to a size_t which will be set to the number of columns in the result.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBGetColumnCount(HiveResultSet* resultset, size_t* col_count, char* err_buf,
              size_t err_buf_len);

/**
 * @brief Construct a HiveColumnDesc.
 *
 * Constructs a HiveColumnDesc with information about a specified column in the resultset.
 * Caller takes ownership of returned HiveColumnDesc and is responsible for deallocating
 * the object by calling DBCloseColumnDesc.
 *
 * @see DBCloseColumnDesc()
 *
 * @param resultset       A HiveResultSet context from which to construct the HiveColumnDesc.
 * @param column_idx      Zero offset index of a column.
 * @param column_desc_ptr A pointer to a HiveColumnDesc pointer which will receive the new
 *                        HiveColumnDesc.
 * @param err_buf         Buffer to receive an error message if HIVE_ERROR is returned.
 *                        NULL can be used if the caller does not care about the error message.
 * @param err_buf_len     Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBCreateColumnDesc(HiveResultSet* resultset, size_t column_idx,
                HiveColumnDesc** column_desc_ptr, char* err_buf, size_t err_buf_len);

/**
 * @brief Find the size of a field as a string.
 *
 * Determines the number of characters needed to display a field stored in the fetched row
 * of a HiveResultSet.
 *
 * @param resultset   An initialized HiveResultSet.
 * @param column_idx  Zero offset index of a column.
 * @param col_len     Pointer to an size_t which will be set to the byte length of the specified field.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBGetFieldDataLen(HiveResultSet* resultset, size_t column_idx, size_t* col_len,
               char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as a C string.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a C String.
 *
 * @param resultset       An initialized HiveResultSet.
 * @param column_idx      Zero offset index of a column.
 * @param buffer          Pointer to a buffer that will receive the data.
 * @param buffer_len      Number of bytes allocated to buffer.
 * @param data_byte_size  Pointer to an size_t which will contain the length of the data available
 *                        to return before calling this function. Can be set to NULL if this
 *                        information is not needed.
 * @param is_null_value   Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                        or 0 otherwise.
 * @param err_buf         Buffer to receive an error message if HIVE_ERROR is returned.
 *                        NULL can be used if the caller does not care about the error message.
 * @param err_buf_len     Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful and there is no more data to fetch
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_SUCCESS_WITH_MORE_DATA if data has been successfully fetched, but there is still
 *           more data to get
 *         HIVE_NO_MORE_DATA if this field has already been completely fetched
 */
HiveReturn DBGetFieldAsCString(HiveResultSet* resultset, size_t column_idx, char* buffer,
                 size_t buffer_len, size_t* data_byte_size, int* is_null_value,
                 char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as a double.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a double
 *       (platform specific).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to a double that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsDouble(HiveResultSet* resultset, size_t column_idx, double* buffer,
                int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as an int.
 *
 * Reads out a field from the currently fetched rowset in a resultset as an int
 *       (platform specific).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to an int that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsInt(HiveResultSet* resultset, size_t column_idx, int* buffer,
               int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as a long int.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a long int
 *       (platform specific).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to a long int that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsLong(HiveResultSet* resultset, size_t column_idx, long* buffer,
              int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as an unsigned long int.
 *
 * Reads out a field from the currently fetched rowset in a resultset as an unsigned long int
 *       (platform specific).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to an unsigned long int that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsULong(HiveResultSet* resultset, size_t column_idx, unsigned long* buffer,
               int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as an int64_t.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a int64_t
 *       (platform independent).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to a int64_t that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsI64(HiveResultSet* resultset, size_t column_idx, int64_t* buffer,
               int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a field as an uint64_t.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a uint64_t
 *       (platform independent).
 *
 * @param resultset     An initialized HiveResultSet.
 * @param column_idx    Zero offset index of a column.
 * @param buffer        Pointer to a uint64_t that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err_buf       Buffer to receive an error message if HIVE_ERROR is returned.
 *                      NULL can be used if the caller does not care about the error message.
 * @param err_buf_len   Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful.
 *         HIVE_ERROR if an error occurred (error messages will be stored in err_buf)
 *         HIVE_NO_MORE_DATA if this field has already been fetched
 */
HiveReturn DBGetFieldAsI64U(HiveResultSet* resultset, size_t column_idx, uint64_t* buffer,
              int* is_null_value, char* err_buf, size_t err_buf_len);

/**
 * @brief Destroys a HiveColumnDesc object.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc object to be removed from memory.
 * @param err_buf     Buffer to receive an error message if HIVE_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return HIVE_SUCCESS if successful, or HIVE_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
HiveReturn DBCloseColumnDesc(HiveColumnDesc* column_desc, char* err_buf, size_t err_buf_len);

/**
 * @brief Get a column name.
 *
 * Retrieves the column name of the associated column in the result table from
 * a HiveColumnDesc.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 * @param buffer      Pointer to a buffer that will receive the column name as a C string
 * @param buffer_len  Number of bytes allocated to buffer
 *
 * @return void
 */
void DBGetColumnName(HiveColumnDesc* column_desc, char* buffer, size_t buffer_len);

/**
 * @brief Get the column type name.
 *
 * Retrieves the name of the column type of the associated column in the result table
 * from a HiveColumnDesc.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 * @param buffer      Pointer to a buffer that will receive the column type name as a C string
 * @param buffer_len  Number of bytes allocated to buffer
 *
 * @return void
 */
void DBGetColumnType(HiveColumnDesc* column_desc, char* buffer, size_t buffer_len);

/**
 * @brief Get the column type as a HiveType.
 *
 * Retrieves the column type as a HiveType for the associated column in the result table
 * from a HiveColumnDesc.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 *
 * @return HiveType of the column.
 */
HiveType DBGetHiveType(HiveColumnDesc* column_desc);

/**
 * @brief Finds whether a column is nullable.
 *
 * Determines from a HiveColumnDesc whether a particular column in the result table is
 * able to contain NULLs.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 *
 * @return 1 if the column can contain NULLs, or
 *         0 if the column cannot contain NULLs
 */
int DBGetIsNullable(HiveColumnDesc* column_desc);

/**
 * @brief Finds whether a column is case sensitive.
 *
 * Determines from a HiveColumnDesc whether a particular column in the result table contains case
 * sensitive data
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 *
 * @return 1 if the column data is case sensitive, or
 *         0 otherwise
 */
int DBGetIsCaseSensitive(HiveColumnDesc* column_desc);

/**
 * @brief Finds the max display size of a column's fields.
 *
 * From a HiveColumnDesc, determines the maximum number of characters needed to represent a
 * field within this column.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 *
 * @return The maximum number of characters needed to represent a field within this column.
 */
size_t DBGetMaxDisplaySize(HiveColumnDesc* column_desc);

/**
 * @brief Finds the max (native) byte size of a column's fields.
 *
 * From a HiveColumnDesc, determines the number of bytes needed to store a field within this
 * column in its native type.
 *
 * @see DBCreateColumnDesc()
 *
 * @param column_desc A HiveColumnDesc associated with the column in question.
 *
 * @return The number of bytes needed to store a field within this column in its native type.
 */
size_t DBGetFieldByteSize(HiveColumnDesc* column_desc);

#ifdef __cplusplus
} // extern "C"
#endif // __cpluscplus


#endif // __hive_client_h__
