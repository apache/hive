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

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include "hiveclient.h"

/* INSTRUCTIONS:
 *   This test suite should have been compiled when 'make' was executed from the
 *   base odbc directory as HiveClientTestC before running this test suite, make
 *   sure to copy the odbc/testdata/ directory to /tmp/ on the same machine as
 *   the Hive Server. To run this test suite, just run the executable: HiveClientTestC.
 */

#define MAX_QUERY_LEN 1024
#define MAX_FIELD_LEN 255

// Currently all Hive numeric types are signed (64-bit platform specific)
// but tests should also run in 32-bit mode
#define MIN_TINYINT -128
#define MIN_SMALLINT -32768
#define MIN_INT 0x80000000
#define MIN_BIGINT 0x8000000000000000
#define SMALLEST_POS_FLOAT 1.1754944E-38
#define SMALLEST_POS_DOUBLE 2.225073858507201E-308
#define MAX_TINYINT 127
#define MAX_SMALLINT 32767
#define MAX_INT 0x7FFFFFFF
#define MAX_BIGINT 0x7FFFFFFFFFFFFFFF
#define BIGGEST_POS_FLOAT 3.4028235E+38
#define BIGGEST_POS_DOUBLE 1.797693134862315E+308

// Convert a macro value to a string
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

// Path to test data (should be supplied at compile time)
#ifdef TEST_DATA_DIR
#define TEST_DATA_DIR_STR STRINGIFY(TEST_DATA_DIR)
#else
#define TEST_DATA_DIR_STR "/tmp/testdata" // Provide a default if not defined
#endif

/**
 * Checks an error condition, and if true:
 * 1. prints the error to stderr
 * 2. returns the specified ret_val
 */
#define RETURN_ON_ASSERT_ONE_ARG(condition, err_format, arg, ret_val) {                         \
  if (condition) {                                                                              \
    fprintf(stderr, "----LINE %i: ", __LINE__);                                                 \
    fprintf(stderr, err_format, arg);                                                           \
    return ret_val;                                                                             \
  }                                                                                             \
}

/**
 * Checks an error condition, and if true:
 * 1. prints the error to stderr
 * 2. closes the DB connection on db_conn
 * 3. returns the specified ret_val
 */
#define RETURN_ON_ASSERT_NO_ARG_CLOSE(condition, err_format, db_conn, ret_val) {                \
  if (condition) {                                                                              \
    char error_buffer_[MAX_HIVE_ERR_MSG_LEN];                                                   \
    fprintf(stderr, "----LINE %i: ", __LINE__);                                                 \
    fprintf(stderr, err_format);                                                                \
    DBCloseConnection(db_conn, error_buffer_, sizeof(error_buffer_));                           \
    return ret_val;                                                                             \
  }                                                                                             \
}
#define RETURN_ON_ASSERT_ONE_ARG_CLOSE(condition, err_format, arg, db_conn, ret_val) {          \
  if (condition) {                                                                              \
    char error_buffer_[MAX_HIVE_ERR_MSG_LEN];                                                   \
    fprintf(stderr, "----LINE %i: ", __LINE__);                                                 \
    fprintf(stderr, err_format, arg);                                                           \
    DBCloseConnection(db_conn, error_buffer_, sizeof(error_buffer_));                           \
    return ret_val;                                                                             \
  }                                                                                             \
}
#define RETURN_ON_ASSERT_TWO_ARG_CLOSE(condition, err_format, arg1, arg2, db_conn, ret_val) {   \
  if (condition) {                                                                              \
    char error_buffer_[MAX_HIVE_ERR_MSG_LEN];                                                   \
    fprintf(stderr, "----LINE %i: ", __LINE__);                                                 \
    fprintf(stderr, err_format, arg1, arg2);                                                    \
    DBCloseConnection(db_conn, error_buffer_, sizeof(error_buffer_));                           \
    return ret_val;                                                                             \
  }                                                                                             \
}

/**************************************************************************************************
 * HELPER FUNCTIONS
 **************************************************************************************************/

int dummyHiveTypeConverter(HiveType type) {
  return 1; // For testing purposes, just return an arbitrary value
}

HiveReturn dropTable(HiveConnection* connection, const char* table_name) {
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  int has_results;
  HiveResultSet* resultset;

  sprintf(query, "DROP TABLE %s", table_name);
  retval = DBExecute(connection, query, &resultset, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, HIVE_ERROR);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, HIVE_ERROR);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(has_results,
      "Query '%s' generated results\n",
      query, connection, HIVE_ERROR);

  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, HIVE_ERROR)

  return HIVE_SUCCESS;
}

/**************************************************************************************************
 * TEST FUNCTIONS
 **************************************************************************************************/

int basic_connect_disconnect_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  HiveReturn retval;
  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  if (connection == NULL) {
    /* If this fails, make sure that Hive server is running with the connect parameter arguments */
    fprintf(stderr, "Connect failed: %s\n", err_buf);
    fprintf(stderr, "\n\n\nMAKE SURE YOU HAVE THE STANDALONE HIVESERVER RUNNING!\n");
    fprintf(stderr, "Expected Connection Parameters:\n");
    fprintf(stderr, "HOST: %s\n", DEFAULT_HOST);
    fprintf(stderr, "PORT: %s\n", DEFAULT_PORT);
    fprintf(stderr, "DATABASE: %s\n\n\n", DEFAULT_DATABASE);
    assert(connection != NULL);
  }
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "Disconnect failed: %s\n",
      err_buf, 0)
  return 1;
}

int basic_query_exec_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  HiveResultSet* resultset;
  HiveReturn retval;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  /* If this fails, make sure that Hive server is running with the connect parameter arguments */
  assert(connection != NULL);

  retval = DBExecute(connection, "SHOW TABLES", &resultset, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);

  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, connection, 0);
  return 1;
}

int basic_fetch_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  int has_results;
  HiveResultSet* resultset;
  HiveReturn retval;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  /* If this fails, make sure that Hive server is running with the connect parameter arguments */
  assert(connection != NULL);

  retval = DBExecute(connection, "SHOW TABLES", &resultset, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);

  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_SUCCESS && !has_results,
      "DBFetch failed: Row fetched but no results detected\n",
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA && has_results,
      "DBFetch failed: Results detected but no rows fetched\n",
      connection, 0)

  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);

  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, connection, 0);
  return 1;
}

int show_tables_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  const char* table_name = "ehwang_tmp_test";
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  char field[MAX_FIELD_LEN];
  int has_results;
  size_t col_count;
  size_t col_len;
  size_t data_byte_size;
  int is_null_value;
  HiveResultSet* resultset;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  assert(connection != NULL); /* If this fails, make sure that Hive server is running with the connect parameter arguments */

  // Drop pre-existing tables of the same name
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Create the table
  sprintf(query, "CREATE TABLE %s (key int, value string)", table_name);
  retval = DBExecute(connection, query, &resultset, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(has_results,
      "Query '%s' generated results\n",
      query, connection, 0);

  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);

  // Test 'show tables' query
  sprintf(query, "SHOW TABLES '%s'", table_name);
  retval = DBExecute(connection, query, &resultset, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(!has_results,
      "Query '%s'could not find table '%s'\n",
      query, table_name, connection, 0);

  retval = DBGetColumnCount(resultset, &col_count, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetColumnCount failed: '%s'\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(col_count != 1,
      "Column count not equal to one: %zu\n",
      col_count, connection, 0);

  // Fetch row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the only row\n",
      connection, 0);

  // Check row data
  retval = DBGetFieldAsCString(resultset, 0, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, table_name) != 0,
      "Tables do not have the same name: table_name:%s, field:%s\n",
      table_name, field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 0, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Fetch row (check that there is nothing else to fetch)
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "DBFetch failed: Only one row, but fetched two\n",
      connection, 0);

  // Clean up table
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Close the handles
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "Disconnect failed: %s\n",
      err_buf, 0)

  return 1;
}

int query_fetch_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  const char* table_name = "ehwang_tmp_test";
  const char* test_data_path = TEST_DATA_DIR_STR "/dataset1.input";
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  char string_field[MAX_FIELD_LEN];
  int int_field;
  int has_results;
  size_t col_count;
  size_t col_len;
  size_t data_byte_size;
  int is_null_value;
  HiveResultSet* resultset;
  HiveColumnDesc* column_desc;
  HiveType hive_type;
  HiveType expected_hive_type;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  /* If this fails, make sure that Hive server is running with the connect parameter arguments */
  assert(connection != NULL);

  // Drop pre-existing tables of the same name
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Create the table
  sprintf(query, "CREATE TABLE %s (key int, value string) STORED AS TEXTFILE", table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Load data into the table
  // NOTE: test_data_path has to be local to the hive server
  // NOTE: test_data_path is a ctrl-A separated file with two fields per line
  sprintf(query, "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", test_data_path, table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Run Select * query
  sprintf(query, "SELECT * FROM %s", table_name);
  // max_buf_len value of 1 to test client side result buffer fetching
  retval = DBExecute(connection, query, &resultset, 1, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(!has_results,
      "Query '%s'should have found results in table: '%s'\n",
      query, table_name, connection, 0);

  retval = DBGetColumnCount(resultset, &col_count, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetColumnCount failed: '%s'\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(col_count != 2,
      "Column count not equal to two: %zu\n",
      col_count, connection, 0);

  // Check HiveType of first column
  retval = DBCreateColumnDesc(resultset, 0, &column_desc, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCreateColumnDesc failed: %s\n",
      err_buf, connection, 0);
  hive_type = DBGetHiveType(column_desc);
  expected_hive_type = HIVE_INT_TYPE;
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(hive_type != expected_hive_type,
      "DBCreateColumnDesc failed: incorrect hive type: %i, expected: %i\n",
      hive_type, expected_hive_type, connection, 0);

  retval = DBCloseColumnDesc(column_desc, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseColumnDesc failed: %s\n",
      err_buf, connection, 0);

  // Check HiveType of second column
  retval = DBCreateColumnDesc(resultset, 1, &column_desc, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCreateColumnDesc failed: %s\n",
      err_buf, connection, 0);
  hive_type = DBGetHiveType(column_desc);
  expected_hive_type = HIVE_STRING_TYPE;
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(hive_type != expected_hive_type,
      "DBCreateColumnDesc failed: incorrect hive type: %i, expected: %i\n",
      hive_type, expected_hive_type, connection, 0);

  retval = DBCloseColumnDesc(column_desc, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseColumnDesc failed: %s\n",
      err_buf, connection, 0);

  // Fetch row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the first row\n",
      connection, 0);

  // Check first row of data
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "Int field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != 1,
      "Int field for row 1 does not match: expected=%i, recieved=%i\n",
      1, int_field, connection, 0);

  retval = DBGetFieldAsCString(resultset, 1, string_field, sizeof(string_field), &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "String field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(string_field, "foo") != 0,
      "String fields does not match: expected='%s', received='%s'\n",
      "foo", string_field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 1, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(string_field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as expected length (%zu)\n",
      col_len, strlen(string_field), connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != col_len,
      "DBGetFieldDataLen failed: Returned data_byte_length (%zu) was not the same as the amount written to buffer (%zu)\n",
      data_byte_size, strlen(string_field) + 1, connection, 0);

  // Fetch second row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the second row\n",
      connection, 0);

  // Check second row of data
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "Int field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != 2,
      "Int field for row 1 does not match: expected=%i, recieved=%i\n",
      1, int_field, connection, 0);

  retval = DBGetFieldAsCString(resultset, 1, string_field, sizeof(string_field), &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "String field for row 2 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(string_field, "bar") != 0,
      "String fields does not match: expected='%s', received='%s'\n",
      "bar", string_field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 1, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(string_field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as expected length (%zu)\n",
      col_len, strlen(string_field), connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != col_len,
      "DBGetFieldDataLen failed: Returned data_byte_length (%zu) was not the same as the amount written to buffer (%zu)\n",
      data_byte_size, strlen(string_field) + 1, connection, 0);

  // Fetch non-existant row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "DBFetch failed: Only 2 rows, but was able to fetch a third...\n",
      connection, 0);

  // Clean up table
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Close the handles
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, 0);

  return 1;
}

int numeric_range_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  const char* table_name = "ehwang_tmp_test";
  const char* test_data_path = TEST_DATA_DIR_STR "/dataset_types.input";
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  int int_field;
  int64_t I64_field;
  double double_field;
  int is_null_value;
  HiveResultSet* resultset;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  /* If this fails, make sure that Hive server is running with the connect parameter arguments */
  assert(connection != NULL);

  // Drop pre-existing tables of the same name
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Create the table
  sprintf(
          query,
          "CREATE TABLE %s (tinyint_type tinyint, smallint_type smallint, int_type int, bigint_type bigint, float_type float, double_type double, null_test int) STORED AS TEXTFILE",
          table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Load data into the table
  // NOTE: test_data_path has to be local to the hive server
  // NOTE: test_data_path is a ctrl-A separated file with seven fields per line
  sprintf(query, "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", test_data_path, table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Run Select * query
  sprintf(query, "SELECT * FROM %s", table_name);
  retval = DBExecute(connection, query, &resultset, 1, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Fetch row of minimum numeric values
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the first row\n",
      connection, 0);

  // Check first row of minimum numeric values

  // Min tinyint field
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "TINYINT field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MIN_TINYINT,
      "TINYINT field for row 1 does not match: expected=%i, received=%i\n",
      MIN_TINYINT, int_field, connection, 0);

  // Min smallint field
  retval = DBGetFieldAsInt(resultset, 1, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "SMALLINT field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MIN_SMALLINT,
      "SMALLINT field for row 1 does not match: expected=%i, received=%i\n",
      MIN_SMALLINT, int_field, connection, 0);

  // Min int field
  retval = DBGetFieldAsInt(resultset, 2, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "INTEGER field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MIN_INT,
      "INTEGER field for row 1 does not match: expected=%i, received=%i\n",
      MIN_INT, int_field, connection, 0);

  // Min bigint field
  retval = DBGetFieldAsI64(resultset, 3, &I64_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsI64 failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "BIGINT field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(I64_field != MIN_BIGINT,
      "BIGINT field for row 1 does not match: expected=%lld, received=%lld\n",
      (long long) MIN_BIGINT, (long long) I64_field, connection, 0);

  // Smallest positive float field
  retval
      = DBGetFieldAsDouble(resultset, 4, &double_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsDouble failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "FLOAT field for row 1 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE((float) double_field != (float) SMALLEST_POS_FLOAT,
      "FLOAT field for row 1 does not match: expected=%.17g, received=%.17g\n",
      (float) SMALLEST_POS_FLOAT, (float) double_field, connection, 0);

  // Smallest positive double field
  retval
      = DBGetFieldAsDouble(resultset, 5, &double_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,"DBGetFieldAsDouble failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "DOUBLE field for row 1 should not be NULL\n", connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE((double) double_field != (double) SMALLEST_POS_DOUBLE,
      "DOUBLE field for row 1 does not match: expected=%.17g, received=%.17g\n",
      (double) SMALLEST_POS_DOUBLE, (double) double_field, connection, 0);

  // NULL field
  retval = DBGetFieldAsInt(resultset, 6, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBGetFieldAsInt failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(!is_null_value,
      "Field should be NULL\n", connection, 0);

  // Fetch row of maximum numeric values
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBFetch failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA, "DBFetch failed: Could not fetch the second row\n",
      connection, 0);

  // Check second row of maximum numeric values

  // Max tinyint field
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBGetFieldAsInt failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "TINYINT field for row 2 should not be NULL\n", connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MAX_TINYINT,
      "TINYINT field for row 2 does not match: expected=%i, received=%i\n", MAX_TINYINT,
      int_field, connection, 0);

  // Max smallint field
  retval = DBGetFieldAsInt(resultset, 1, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBGetFieldAsInt failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "SMALLINT field for row 2 should not be NULL\n", connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MAX_SMALLINT,
      "SMALLINT field for row 2 does not match: expected=%i, received=%i\n",
      MAX_SMALLINT, int_field, connection, 0);

  // Max int field
  retval = DBGetFieldAsInt(resultset, 2, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBGetFieldAsInt failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "INTEGER field for row 2 should not be NULL\n", connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_field != MAX_INT,
      "INTEGER field for row 2 does not match: expected=%i, received=%i\n",
      MAX_INT, int_field, connection, 0);

  // Max bigint field
  retval = DBGetFieldAsI64(resultset, 3, &I64_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR, "DBGetFieldAsI64 failed: %s\n", err_buf,
      connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "BIGINT field for row 2 should not be NULL\n", connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(I64_field != MAX_BIGINT,
      "BIGINT field for row 2 does not match: expected=%lld, received=%lld\n",
      (long long) MAX_BIGINT, (long long) I64_field, connection, 0);

  // Biggest positive float field
  retval
      = DBGetFieldAsDouble(resultset, 4, &double_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsDouble failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "FLOAT field for row 2 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE((float) double_field != (float) BIGGEST_POS_FLOAT,
      "FLOAT field for row 2 does not match: expected=%.17g, received=%.17g\n",
      (float) BIGGEST_POS_FLOAT, (float) double_field, connection, 0);

  // Biggest positive double field
  retval
      = DBGetFieldAsDouble(resultset, 5, &double_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsDouble failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(is_null_value,
      "DOUBLE field for row 2 should not be NULL\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE((double) double_field != (double) BIGGEST_POS_DOUBLE,
      "DOUBLE field for row 2 does not match: expected=%.17g, received=%.17g\n",
      (double) BIGGEST_POS_DOUBLE, (double) double_field, connection, 0);

  // NULL field
  retval = DBGetFieldAsInt(resultset, 6, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(!is_null_value,
      "Field should be NULL\n",
      connection, 0);

  // Clean up table
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Close the handles
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, 0);

  return 1;
}

int field_multifetch_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  const char* table_name = "ehwang_tmp_test";
  const char* test_data_path = TEST_DATA_DIR_STR "/dataset2.input";
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  char string_field[MAX_FIELD_LEN];
  size_t data_byte_size;
  int int_field;
  int is_null_value;
  HiveResultSet* resultset;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  /* If this fails, make sure that Hive server is running with the connect parameter arguments */
  assert(connection != NULL);

  // Drop pre-existing tables of the same name
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Create the table
  sprintf(query, "CREATE TABLE %s (fixed_len_field int, var_len_field string) STORED AS TEXTFILE",
          table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Load data into the table
  // NOTE: test_data_path has to be local to the hive server
  // NOTE: test_data_path is a ctrl-A separated file with two fields per line
  sprintf(query, "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", test_data_path, table_name);
  retval = DBExecute(connection, query, NULL, 0, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Run Select * query
  sprintf(query, "SELECT * FROM %s", table_name);
  retval = DBExecute(connection, query, &resultset, 1, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Fetch row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the first row\n",
      connection, 0);

  // Test multi-fetches on fixed length fields:
  // First fetch
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS,
      "First fetch of a fixed length field should be successful\n",
      connection, 0);

  // Second fetch
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Second consecutive fetch of a fixed length field should return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Third fetch
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Third consecutive fetch of a fixed length field should return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Test multi-fetches on var length fields:
  // First fetch with sufficient buffer space
  retval = DBGetFieldAsCString(resultset, 1, string_field, sizeof(string_field), &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  /* Length of alphabet string in data */
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != 26,
      "DBGetFieldAsCString failed: incorrect data_byte_size: expected=%zu, received=%zu\n",
      (size_t) 26, data_byte_size, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS,
      "First fetch of a variable length field with sufficient buffer space should be successful\n",
      connection, 0);

  // Second fetch with sufficient buffer space
  retval = DBGetFieldAsCString(resultset, 1, string_field, sizeof(string_field), &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Second consecutive fetch of a completely fetched variable length field should return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Third fetch with sufficient buffer space
  retval = DBGetFieldAsCString(resultset, 1, string_field, sizeof(string_field), &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Third consecutive fetch of a completely fetched variable length field should return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Test multi-fetches on fixed length fields after reset column reset:
  // First fetch
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS,
      "First fetch of a fixed length field after a column reset should be successful\n",
      connection, 0);
  // Second fetch
  retval = DBGetFieldAsInt(resultset, 0, &int_field, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Second consecutive fetch of a fixed length field should always return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Test multi-fetches on var length fields after column reset with insufficient buffer space:
  // First fetch with insufficient buffer space after a column reset
  size_t reduced_buffer_size = 13; // For a source data size of 26 bytes
  retval = DBGetFieldAsCString(resultset, 1, string_field, reduced_buffer_size, &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  /* Length of available alphabet string to return before truncation */
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != 26,
      "DBGetFieldAsCString failed: incorrect data_byte_size: expected=%zu, received=%zu\n",
      (size_t) 26, data_byte_size, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS_WITH_MORE_DATA,
      "First fetch of a variable length field with insufficient buffer space should return HIVE_SUCCESS_WITH_MORE_DATA\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(string_field, "abcdefghijkl") != 0,
      "Fetched string does not match: expected:%s, received:%s\n",
      "abcdefghijkl", string_field, connection, 0);

  // Second fetch with insufficient buffer space
  retval = DBGetFieldAsCString(resultset, 1, string_field, reduced_buffer_size, &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  /* Length of remaining alphabet string to return before truncation */
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != 14,
      "DBGetFieldAsCString failed: incorrect data_byte_size: expected=%zu, received=%zu\n",
      (size_t) 14, data_byte_size, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS_WITH_MORE_DATA,
      "Second consecutive fetch with still insufficient buffer space should return HIVE_SUCCESS_WITH_MORE_DATA\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(string_field, "mnopqrstuvwx"),
      "Fetched string does not match: expected:%s, received:%s\n",
      "mnopqrstuvwx", string_field, connection, 0);

  // Third fetch to obtain the remainder of the buffer data
  retval = DBGetFieldAsCString(resultset, 1, string_field, reduced_buffer_size, &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  /* Length of remaining alphabet string to return before truncation */
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(data_byte_size != 2,
      "DBGetFieldAsCString failed: incorrect data_byte_size: expected=%zu, received=%zu\n",
      (size_t) 2, data_byte_size, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_SUCCESS,
      "Fetch that returns all or the complete remainder of the data buffer should return HIVE_SUCCESS\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(string_field, "yz") != 0,
      "Fetched string does not match: expected:%s, received:%s\n",
      "yz", string_field, connection, 0);

  // Fourth fetch after all data has already been obtained
  retval = DBGetFieldAsCString(resultset, 1, string_field, reduced_buffer_size, &data_byte_size,
                               &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "Fourth consecutive fetch of a completely fetched variable length field should return HIVE_NO_MORE_DATA\n",
      connection, 0);

  // Clean up table
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Close the handles
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, 0);

  return 1;
}

int meta_data_function_test() {
  fprintf(stderr, "Running %s...\n", __FUNCTION__);
  char err_buf[MAX_HIVE_ERR_MSG_LEN];
  const char* table_name = "ehwang_tmp_test";
  HiveReturn retval;
  char query[MAX_QUERY_LEN];
  char field[MAX_FIELD_LEN];
  int has_results;
  int int_buffer;
  size_t col_count;
  size_t col_len;
  size_t data_byte_size;
  int is_null_value;
  HiveResultSet* resultset;

  HiveConnection* connection = DBOpenConnection(DEFAULT_DATABASE, DEFAULT_HOST, atoi(DEFAULT_PORT),
                                                atoi(DEFAULT_FRAMED), err_buf, sizeof(err_buf));
  assert(connection != NULL); /* If this fails, make sure that Hive server is running with the connect parameter arguments */

  // Drop pre-existing tables of the same name
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Create the table
  sprintf(query, "CREATE TABLE %s (key int, value string)", table_name);
  retval = DBExecute(connection, query, NULL, 10, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBExecute failed: %s\n",
      err_buf, connection, 0);

  // Test DBTables
  retval = DBTables(connection, table_name, &resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBTables failed: %s\n",
      err_buf, connection, 0);
  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(!has_results,
      "DBTables could not find table '%s'\n",
      table_name, connection, 0);

  retval = DBGetColumnCount(resultset, &col_count, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetColumnCount failed: '%s'\n",
      err_buf, connection, 0);
  /* The DBTables function should always return a result set with exactly 5 columns */
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(col_count != 5,
      "Column count not equal to five: %zu\n",
      col_count, connection, 0);

  // Fetch row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the only row\n",
      connection, 0);

  // Check row data
  // Index 2 (a.k.a column 3) in a row will always contain the name of the table
  retval = DBGetFieldAsCString(resultset, 2, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, table_name) != 0,
      "Tables do not have the same name: table_name:%s, field:%s\n",
      table_name, field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 2, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Fetch row (check that there is nothing else to fetch)
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "DBFetch failed: Only one row, but fetched two\n",
      connection, 0);

  // Close the resultset
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);

  // Test DBColumns

  // Match all columns in created table
  retval = DBColumns(connection, &dummyHiveTypeConverter, table_name, "*", &resultset, err_buf,
                     sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBColumns failed: %s\n",
      err_buf, connection, 0);

  retval = DBHasResults(resultset, &has_results, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBHasResults failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(!has_results,
      "DBColumns could not find columns for table '%s'\n",
      table_name, connection, 0);

  retval = DBGetColumnCount(resultset, &col_count, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetColumnCount failed: '%s'\n",
      err_buf, connection, 0);
  /* The DBColumns function should always return a result set with exactly 18 columns */
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(col_count != 18,
      "Column count not equal to 18: %zu\n",
      col_count, connection, 0);

  // Fetch first row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the only row\n",
      connection, 0);

  // Check first row data
  // Index 2 (a.k.a column 3) in a row will always contain the name of the table
  retval = DBGetFieldAsCString(resultset, 2, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, table_name) != 0,
      "Tables do not have the same name: table_name:%s, field:%s\n",
      table_name, field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 2, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Index 3 (a.k.a column 4) in a row will always contain the name of the column
  retval = DBGetFieldAsCString(resultset, 3, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, "key") != 0,
      "Columns do not have the same name: column_name:%s, field:%s\n",
      "key", field, connection, 0);
  retval = DBGetFieldDataLen(resultset, 3, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Index 4 (a.k.a column 5) in a row will always contain the SQL data type value
  retval = DBGetFieldAsInt(resultset, 4, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 1,
      "Wrong converted SQL type: expected=%i, received=%i\n",
      1, int_buffer, connection, 0);

  // Index 13 (a.k.a column 14) in a row will also always contain the SQL data type value
  retval = DBGetFieldAsInt(resultset, 13, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 1,
      "Wrong converted SQL data type: expected=%i, received=%i\n",
      1, int_buffer, connection, 0);

  // Index 16 (a.k.a column 17) in a row will always contain the column ordinal position
  retval = DBGetFieldAsInt(resultset, 16, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 1,
      "Wrong ordinal position: expected=%i, received=%i\n",
      1, int_buffer, connection, 0);

  // Fetch second row
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_NO_MORE_DATA,
      "DBFetch failed: Could not fetch the only row\n",
      connection, 0);

  // Check second row data
  // Index 2 (a.k.a column 3) in a row will always contain the name of the table
  retval = DBGetFieldAsCString(resultset, 2, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, table_name) != 0,
      "Tables do not have the same name: table_name:%s, field:%s\n",
      table_name, field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 2, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Index 3 (a.k.a column 4) in a row will always contain the name of the column
  retval = DBGetFieldAsCString(resultset, 3, field, sizeof(field), &data_byte_size, &is_null_value,
                               err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsCString failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(strcmp(field, "value") != 0,
      "Columns do not have the same name: column_name:%s, field:%s\n",
      "key", field, connection, 0);

  retval = DBGetFieldDataLen(resultset, 3, &col_len, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldDataLen failed: Could not get the strlen\n",
      connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(col_len != strlen(field),
      "DBGetFieldDataLen failed: Returned length (%zu) was not the same as table_name (%zu)\n",
      col_len, strlen(field), connection, 0);

  // Index 4 (a.k.a column 5) in a row will always contain the SQL data type value
  retval = DBGetFieldAsInt(resultset, 4, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 1,
      "Wrong converted SQL type: expected=%i, received=%i\n",
      1, int_buffer, connection, 0);

  // Index 13 (a.k.a column 14) in a row will also always contain the SQL data type value
  retval = DBGetFieldAsInt(resultset, 13, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 1,
      "Wrong converted SQL data type: expected=%i, received=%i\n",
      1, int_buffer, connection, 0);

  // Index 16 (a.k.a column 17) in a row will always contain the column ordinal position
  retval = DBGetFieldAsInt(resultset, 16, &int_buffer, &is_null_value, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBGetFieldAsInt failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_TWO_ARG_CLOSE(int_buffer != 2,
      "Wrong ordinal position: expected=%i, received=%i\n",
      2, int_buffer, connection, 0);

  // Fetch row (check that there is nothing else to fetch)
  retval = DBFetch(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBFetch failed: %s\n",
      err_buf, connection, 0);
  RETURN_ON_ASSERT_NO_ARG_CLOSE(retval != HIVE_NO_MORE_DATA,
      "DBFetch failed: Only one row, but fetched two\n",
      connection, 0);

  // Close the resultset
  retval = DBCloseResultSet(resultset, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG_CLOSE(retval == HIVE_ERROR,
      "DBCloseResultSet failed: %s\n",
      err_buf, connection, 0);

  // Clean up table
  if (dropTable(connection, table_name) == HIVE_ERROR) {
    DBCloseConnection(connection, err_buf, sizeof(err_buf));
    return 0;
  }

  // Close the connection
  retval = DBCloseConnection(connection, err_buf, sizeof(err_buf));
  RETURN_ON_ASSERT_ONE_ARG(retval == HIVE_ERROR,
      "DBDisconnect failed: %s\n",
      err_buf, 0);
  return 1;
}

/**************************************************************************************************
 * MAIN FUNCTION
 **************************************************************************************************/

int main() {
  int failed = 0;
  fprintf(stderr, "\nStarting Hive Client C tests...\n\n");

  if (basic_connect_disconnect_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED basic_connect_disconnect_test!\n");
  }
  if (basic_query_exec_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED basic_query_exec_test!\n");
  }
  if (basic_fetch_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED basic_fetch_test!\n");
  }
  if (show_tables_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED show_tables_test!\n");
  }
  if (query_fetch_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED query_fetch_test!\n");
  }
  if (numeric_range_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED numeric_range_test!\n");
  }
  if (field_multifetch_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED field_multifetch_test!\n");
  }
  if (meta_data_function_test() == 0) {
    failed++;
    fprintf(stderr, "----FAILED meta_data_function_test!\n");
  }

  if (failed == 0) {
    fprintf(stderr, "\nALL HIVE CLIENT TESTS PASSED!\n\n");
    return 0;
  } else {
    fprintf(stderr, "\nHIVE CLIENT TEST FAILURE: %i test(s) failed.\n\n", failed);
    return 1;
  }
}

