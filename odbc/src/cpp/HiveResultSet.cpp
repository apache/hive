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

#include "ThriftHive.h"

#include "HiveResultSet.h"
#include "hiveclienthelper.h"
#include "thriftserverconstants.h"


/*************************************************************************************************
 * HiveQueryResultSet Subclass Definition
 ************************************************************************************************/

HiveQueryResultSet::HiveQueryResultSet(int max_buf_rows) {
  m_connection = NULL;
  m_serial_rowset.reset();
  assert(max_buf_rows > 0);
  m_max_buffered_rows = max_buf_rows;
  m_fetch_idx = -1;
  m_has_results = false;
  m_fetch_attempted = false;
  /* Allocate the necessary amount of memory to prevent resizing */
  m_result_set_data.reserve(max_buf_rows);
}

HiveQueryResultSet::~HiveQueryResultSet() {
  /* Nothing to deallocate */
}

HiveReturn HiveQueryResultSet::initialize(HiveConnection* connection, char* err_buf,
                                          size_t err_buf_len) {
  assert(connection != NULL);
  m_connection = connection;
  m_serial_rowset.reset();
  m_fetch_idx = -1;
  m_has_results = false;
  m_fetch_attempted = false;
  return initializeSchema(err_buf, err_buf_len);
}

HiveReturn HiveQueryResultSet::fetchNext(char* err_buf, size_t err_buf_len) {
  m_fetch_idx++;
  if (m_fetch_idx >= (int) m_result_set_data.size()) /* If there are no more buffered rows... */
  {
    /* Repopulate the result buffer */
    if (fetchNewResults(err_buf, err_buf_len) == HIVE_ERROR) {
      return HIVE_ERROR;
    }
    /* Set the cursor to point at the first element (fetchNewResults would have reset its position)*/
    m_fetch_idx = 0;
    if (m_result_set_data.empty()) {
      return HIVE_NO_MORE_DATA; /* No more data to fetch */
    }
  }
  m_serial_rowset.reset(); /* Remove old row data before saving next */
  m_serial_rowset.initialize(m_schema, m_result_set_data[m_fetch_idx]);
  return HIVE_SUCCESS;
}

HiveReturn HiveQueryResultSet::hasResults(int* results, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(results == NULL, __FUNCTION__,
                   "Pointer to has_results (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);

  if (!m_fetch_attempted) {
    if (fetchNewResults(err_buf, err_buf_len) == HIVE_ERROR) {
      return HIVE_ERROR; /* An error must have occurred */
    }
  }

  *results = m_has_results ? 1 : 0; /* This flag will have been set by the fetch call */
  return HIVE_SUCCESS;
}

HiveReturn HiveQueryResultSet::getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(col_count == NULL, __FUNCTION__,
                   "Pointer to col_count (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  /* If m_schema has been initialized, then m_schema.fieldSchemas must be populated */
  *col_count = m_schema.fieldSchemas.size();

  return HIVE_SUCCESS;
}

HiveReturn HiveQueryResultSet::createColumnDesc(size_t column_idx,
                                                HiveColumnDesc** column_desc_ptr, char* err_buf,
                                                size_t err_buf_len) {
  RETURN_ON_ASSERT(column_desc_ptr == NULL, __FUNCTION__,
                   "Pointer to column_desc (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(m_schema.fieldSchemas.empty(), __FUNCTION__,
                   "Resultset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= m_schema.fieldSchemas.size(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  *column_desc_ptr = new HiveColumnDesc();
  (*column_desc_ptr)->initialize(m_schema.fieldSchemas[column_idx]);
  return HIVE_SUCCESS;
}

HiveRowSet& HiveQueryResultSet::getRowSet() {
  return m_serial_rowset;
}

HiveReturn HiveQueryResultSet::initializeSchema(char* err_buf, size_t err_buf_len) {
  try {
    m_connection->client->getSchema(m_schema);
  } catch (Apache::Hadoop::Hive::HiveServerException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                     "Unknown Hive get result schema error.", err_buf, err_buf_len, HIVE_ERROR);
  }

  /* TODO: hard code this in for now because m_schema.properties not properly implemented;
   * but remove this when it is implemented */
  m_schema.properties[FIELD_DELIM] = "\t";
  m_schema.properties[SERIALIZATION_NULL_FORMAT] = DEFAULT_NULL_FORMAT;

  /* TODO: replace the real null representation with 'NULL' because of a bug in the Hive Server
   * fetch function; remove this when Hive Server has been fixed to not replace the actual null
   * rep with NULL. */
  m_schema.properties[SERIALIZATION_NULL_FORMAT] = "NULL";

  /* Verify the presence of known m_schema properties */
  assert(m_schema.properties.find(FIELD_DELIM) != m_schema.properties.end());
  assert(m_schema.properties.find(SERIALIZATION_NULL_FORMAT) != m_schema.properties.end());

  return HIVE_SUCCESS;
}

HiveReturn HiveQueryResultSet::fetchNewResults(char* err_buf, size_t err_buf_len) {
  m_result_set_data.clear(); /* Empty the original buffer just to be safe */
  assert(m_connection != NULL);
  assert(m_connection->client != NULL);
  assert(m_max_buffered_rows > 0);
  try {
    m_connection->client->fetchN(m_result_set_data, m_max_buffered_rows);
  } catch (Apache::Hadoop::Hive::HiveServerException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                     "Unknown Hive FetchN error.", err_buf, err_buf_len, HIVE_ERROR);
  }

  /* This indicates that a Hive server fetch call has successfully executed */
  m_fetch_attempted = true;
  if (!m_result_set_data.empty()) {
    /* This is initialized to be false and will remain true forever once this is set */
    m_has_results = true;
  }
  m_fetch_idx = -1; /* Reset the cursor b/c the old index no longer has any meaning */
  return HIVE_SUCCESS;
}



/*************************************************************************************************
 * HiveTablesResultSet Subclass Definition
 ************************************************************************************************/

/*
 * g_tables_schema: An array of arrays of C strings used to define the expected
 *     resultset schema for HiveTablesResultSet. All values will be stored as
 *     C strings (even numbers) as they will eventually be converted to their
 *     proper types. Each entry of g_tables_schema is an array with the following
 *     format:
 *       1. Column name
 *       2. Column type name
 *       3. Default value (as a C string)
 */
static const char* g_tables_schema[][3] = {
        {"TABLE_CAT"  , STRING_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"TABLE_SCHEM", STRING_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"TABLE_NAME",  STRING_TYPE_NAME, ""},
        {"TABLE_TYPE",  STRING_TYPE_NAME, "TABLE"},
        {"REMARKS",     STRING_TYPE_NAME, ""}
};

HiveTablesResultSet::HiveTablesResultSet() {
  m_fetch_idx = -1;
  m_curr_row_data.reserve(5); /* The resultset will always have 5 columns */
}

HiveTablesResultSet::~HiveTablesResultSet() {
  /* Nothing to deallocate */
}

HiveReturn HiveTablesResultSet::initialize(HiveConnection* connection,
                                           const char* tbl_search_pattern, char* err_buf,
                                           size_t err_buf_len) {
  RETURN_ON_ASSERT(connection == NULL, __FUNCTION__,
                   "Hive connection cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(connection->client == NULL, __FUNCTION__,
                   "Hive connection client cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(tbl_search_pattern == NULL, __FUNCTION__,
                   "Table search pattern cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  m_fetch_idx = -1;
  try {
    /* Just use the default database name for now b/c Hive does not yet support multiple
     * databases */
    connection->client->get_tables(m_tables, DEFAULT_DATABASE, tbl_search_pattern);
  } catch (Apache::Hadoop::Hive::MetaException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                     "Unknown Hive get tables error.", err_buf, err_buf_len, HIVE_ERROR);
  }
  /* Sort the table names */
  sort(m_tables.begin(), m_tables.end());
  return initializeSchema(err_buf, err_buf_len);
}

HiveReturn HiveTablesResultSet::fetchNext(char* err_buf, size_t err_buf_len) {
  m_fetch_idx++;
  if (m_fetch_idx >= (int) m_tables.size()) /* If there are no more tables */
  {
    m_fetch_idx--; /* Prevent the m_fetch_idx from wrapping around after too many calls */
    return HIVE_NO_MORE_DATA; /* No more data to fetch */
  }

  /* Populate m_curr_row_data with the latest row information */
  if (constructCurrentRow(err_buf, err_buf_len) == HIVE_ERROR) {
    return HIVE_ERROR; /* An error must have occurred */
  }
  m_vecstring_rowset.reset(); /* Remove old rowset data before saving next */
  m_vecstring_rowset.initialize(m_schema, &m_curr_row_data);

  return HIVE_SUCCESS;
}

HiveReturn HiveTablesResultSet::hasResults(int* results, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(results == NULL, __FUNCTION__,
                   "Pointer to has_results (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  *results = (m_tables.size() > 0) ? 1 : 0; /* Just check the vector length because no caching */
  return HIVE_SUCCESS;
}

HiveReturn HiveTablesResultSet::getColumnCount(size_t* col_count, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(col_count == NULL, __FUNCTION__,
                   "Pointer to col_count (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  /* If m_schema has been initialized, then m_schema.fieldSchemas must be populated */
  *col_count = m_schema.fieldSchemas.size();
  return HIVE_SUCCESS;
}

HiveReturn HiveTablesResultSet::createColumnDesc(size_t column_idx,
                                                 HiveColumnDesc** column_desc_ptr, char* err_buf,
                                                 size_t err_buf_len) {
  RETURN_ON_ASSERT(column_desc_ptr == NULL, __FUNCTION__,
                   "Pointer to column_desc (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(m_schema.fieldSchemas.empty(), __FUNCTION__,
                   "Resultset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= m_schema.fieldSchemas.size(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  *column_desc_ptr = new HiveColumnDesc();
  (*column_desc_ptr)->initialize(m_schema.fieldSchemas[column_idx]);
  return HIVE_SUCCESS;
}

HiveRowSet& HiveTablesResultSet::getRowSet() {
  return m_vecstring_rowset;
}

HiveReturn HiveTablesResultSet::initializeSchema(char* err_buf, size_t err_buf_len) {
  /* Initialize the schema values needed for this resultset.
   * OK to hardcode because these fields should never change */
  m_schema.properties[SERIALIZATION_NULL_FORMAT] = DEFAULT_NULL_FORMAT;

  Apache::Hadoop::Hive::FieldSchema tmp_field_schema;
  for (unsigned int idx = 0; idx < LENGTH(g_tables_schema); idx++) {
    tmp_field_schema.name = g_tables_schema[idx][0];
    tmp_field_schema.type = g_tables_schema[idx][1];
    /* Makes a copy of this tmp FieldSchema */
    m_schema.fieldSchemas.push_back(tmp_field_schema);
  }

  /* There should be exactly 5 columns for this resultset */
  assert(m_schema.fieldSchemas.size() == 5);

  return HIVE_SUCCESS;
}

HiveReturn HiveTablesResultSet::constructCurrentRow(char* err_buf, size_t err_buf_len) {
  /* Clear out the previous row data just to be safe */
  m_curr_row_data.clear();
  int column_num;
  for (unsigned int idx = 0; idx < LENGTH(g_tables_schema); idx++) {
    /* column_num represents ordinal position instead of index to avoid confusion */
    column_num = idx + 1;
    switch (column_num)
    {
    case 3: // If Col3: TABLE_NAME
      m_curr_row_data.push_back(m_tables[m_fetch_idx]);
      break;

    default: // Add in default value
      m_curr_row_data.push_back(g_tables_schema[idx][2]);
      break;
    }
  }

  /* There should be exactly 5 columns for this resultset */
  assert(m_curr_row_data.size() == 5);

  return HIVE_SUCCESS;
}


/*************************************************************************************************
 * HiveColumnsResultSet Subclass Definition
 ************************************************************************************************/

/*
 * g_columns_schema: An array of arrays of C strings used to define the expected
 *     resultset schema for HiveColumnsResultSet. All values will be stored as
 *     C strings (even numbers) as they will eventually be converted to their
 *     proper types. Each entry of g_columns_schema is an array with the following
 *     format:
 *       1. Column name
 *       2. Column type name
 *       3. Default value (as a C string)
 */
static const char* g_columns_schema[][3] = {
        {"TABLE_CAT"  ,       STRING_TYPE_NAME,   DEFAULT_NULL_FORMAT},
        {"TABLE_SCHEM",       STRING_TYPE_NAME,   DEFAULT_NULL_FORMAT},
        {"TABLE_NAME",        STRING_TYPE_NAME,   ""},
        {"COLUMN_NAME",       STRING_TYPE_NAME,   ""},
        {"DATA_TYPE",         SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"TYPE_NAME",         STRING_TYPE_NAME,   STRING_TYPE_NAME},
        {"COLUMN_SIZE",       INT_TYPE_NAME,      STRINGIFY(MAX_BYTE_LENGTH)},
        {"BUFFER_LENGTH",     INT_TYPE_NAME,      STRINGIFY(MAX_BYTE_LENGTH)},
        {"DECIMAL_DIGITS",    SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"NUM_PREC_RADIX",    SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"NULLABLE",          SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"REMARKS",           STRING_TYPE_NAME,   DEFAULT_NULL_FORMAT},
        {"COLUMN_DEF",        STRING_TYPE_NAME,   "NULL"},
        {"SQL_DATA_TYPE",     SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"SQL_DATETIME_SUB",  SMALLINT_TYPE_NAME, DEFAULT_NULL_FORMAT},
        {"CHAR_OCTET_LENGTH", INT_TYPE_NAME,      "1"},
        {"ORDINAL_POSITION",  INT_TYPE_NAME,      DEFAULT_NULL_FORMAT},
        {"IS_NULLABLE",       STRING_TYPE_NAME,   "YES"}
};

HiveColumnsResultSet::HiveColumnsResultSet(int(*fpHiveToSQLType)(HiveType)) {
  m_connection = NULL;
  assert(fpHiveToSQLType != NULL);
  m_fpHiveToSQLType = fpHiveToSQLType;
  m_tbl_fetch_idx = -1;
  m_col_fetch_idx = -1;
  m_curr_row_data.reserve(18); /* The resultset will always have 18 columns */
}

HiveColumnsResultSet::~HiveColumnsResultSet() {
  /* Nothing to deallocate */
}

HiveReturn HiveColumnsResultSet::initialize(HiveConnection* connection,
                                            const char* tbl_search_pattern,
                                            const char* col_search_pattern, char* err_buf,
                                            size_t err_buf_len) {
  RETURN_ON_ASSERT(connection == NULL, __FUNCTION__,
                   "Hive connection cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(connection->client == NULL, __FUNCTION__,
                   "Hive connection client cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(tbl_search_pattern == NULL, __FUNCTION__,
                   "Table search pattern cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(col_search_pattern == NULL, __FUNCTION__,
                   "Column search pattern cannot be NULL.", err_buf, err_buf_len, HIVE_ERROR);

  /* TODO: col_search_pattern is not currently supported; arg is ignored for now;
   * either add support in Hive Server or here */

  m_connection = connection;
  m_tbl_fetch_idx = -1;
  m_col_fetch_idx = -1;

  try {
    /* Just use the default database name for now b/c Hive does not yet support multiple
     * databases */
    connection->client->get_tables(m_tables, DEFAULT_DATABASE, tbl_search_pattern);
  } catch (Apache::Hadoop::Hive::MetaException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                     "Unknown Hive get tables error.", err_buf, err_buf_len, HIVE_ERROR);
  }
  /* Sort the table names */
  sort(m_tables.begin(), m_tables.end());

  return initializeSchema(err_buf, err_buf_len);
}

HiveReturn HiveColumnsResultSet::fetchNext(char* err_buf, size_t err_buf_len) {
  m_col_fetch_idx++;
  /* If there are no more columns in the current table */
  if (m_col_fetch_idx >= (int) m_columns.size()) {
    HiveReturn retval = getNextTableFields(err_buf, err_buf_len);
    if (retval != HIVE_SUCCESS) {
      /* Prevent the m_col_fetch_idx from wrapping around after too many calls */
      m_col_fetch_idx--;
      return retval;
    }
    /* If getNextTableFields() successful, m_columns must contain new field schemas for
     * next table */
    assert(m_columns.size() > 0);
    /* Set index back to first element (getNextTableColumns() would have reset its value) */
    m_col_fetch_idx = 0;
  }

  /* Populate m_curr_row_data with the latest row information */
  if (constructCurrentRow(err_buf, err_buf_len) == HIVE_ERROR) {
    return HIVE_ERROR; /* An error must have occurred */
  }
  m_vecstring_rowset.reset(); /* Remove old rowset data before saving next */
  m_vecstring_rowset.initialize(m_schema, &m_curr_row_data);

  return HIVE_SUCCESS;
}

HiveReturn HiveColumnsResultSet::hasResults(int* results, char* err_buf, size_t err_buf_len) {
  RETURN_ON_ASSERT(results == NULL, __FUNCTION__,
                   "Pointer to has_results (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  /* If there are tables, then there must be columns to fetch */
  *results = (m_tables.size() > 0) ? 1 : 0;
  return HIVE_SUCCESS;
}

HiveReturn HiveColumnsResultSet::getColumnCount(size_t* col_count, char* err_buf,
                                                size_t err_buf_len) {
  RETURN_ON_ASSERT(col_count == NULL, __FUNCTION__,
                   "Pointer to col_count (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  /* If m_schema has been initialized, then m_schema.fieldSchemas must be populated */
  *col_count = m_schema.fieldSchemas.size();
  return HIVE_SUCCESS;
}

HiveReturn HiveColumnsResultSet::createColumnDesc(size_t column_idx,
                                                  HiveColumnDesc** column_desc_ptr, char* err_buf,
                                                  size_t err_buf_len) {
  RETURN_ON_ASSERT(column_desc_ptr == NULL, __FUNCTION__,
                   "Pointer to column_desc (output) cannot be NULL.", err_buf, err_buf_len,
                   HIVE_ERROR);
  RETURN_ON_ASSERT(m_schema.fieldSchemas.empty(), __FUNCTION__,
                   "Resultset contains zero columns.", err_buf, err_buf_len, HIVE_ERROR);
  RETURN_ON_ASSERT(column_idx >= m_schema.fieldSchemas.size(), __FUNCTION__,
                   "Column index out of bounds.", err_buf, err_buf_len, HIVE_ERROR);

  *column_desc_ptr = new HiveColumnDesc();
  (*column_desc_ptr)->initialize(m_schema.fieldSchemas[column_idx]);
  return HIVE_SUCCESS;
}

HiveRowSet& HiveColumnsResultSet::getRowSet() {
  return m_vecstring_rowset;
}

HiveReturn HiveColumnsResultSet::getNextTableFields(char* err_buf, size_t err_buf_len) {
  /* Clear out the field schemas for the previous table */
  m_columns.clear();

  m_tbl_fetch_idx++;
  if (m_tbl_fetch_idx >= (int) m_tables.size()) /* If there are no more tables */
  {
    /* Prevent the m_tbl_fetch_idx from wrapping around after too many calls */
    m_tbl_fetch_idx--;
    return HIVE_NO_MORE_DATA; /* No more data to fetch */
  }

  assert(m_connection != NULL);
  assert(m_connection->client != NULL);
  try {
    /* Just use the default database name for now b/c Hive does not yet support multiple databases */
    m_connection->client->get_schema(m_columns, DEFAULT_DATABASE, m_tables[m_tbl_fetch_idx]);
  } catch (Apache::Hadoop::Hive::MetaException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (Apache::Hadoop::Hive::UnknownTableException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (Apache::Hadoop::Hive::UnknownDBException& ex) {
    RETURN_FAILURE(__FUNCTION__, ex.what(), err_buf, err_buf_len, HIVE_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
                     "Unknown Hive get fields error.", err_buf, err_buf_len, HIVE_ERROR);
  }
  assert(m_columns.size() > 0); /* Every table must have at least one column */

  return HIVE_SUCCESS;
}

HiveReturn HiveColumnsResultSet::initializeSchema(char* err_buf, size_t err_buf_len) {
  /* Initialize the schema values needed for this resultset.
   * OK to hardcode because these fields should never change */
  m_schema.properties[SERIALIZATION_NULL_FORMAT] = DEFAULT_NULL_FORMAT;

  Apache::Hadoop::Hive::FieldSchema tmp_field_schema;
  for (unsigned int idx = 0; idx < LENGTH(g_columns_schema); idx++) {
    tmp_field_schema.name = g_columns_schema[idx][0];
    tmp_field_schema.type = g_columns_schema[idx][1];
    /* Makes a copy of this tmp FieldSchema */
    m_schema.fieldSchemas.push_back(tmp_field_schema);
  }

  /* There should be exactly 18 columns for this resultset */
  assert(m_schema.fieldSchemas.size() == 18);

  return HIVE_SUCCESS;
}

HiveReturn HiveColumnsResultSet::constructCurrentRow(char* err_buf, size_t err_buf_len) {
  /* Clear out the previous row data just to be safe */
  m_curr_row_data.clear();

  assert(m_fpHiveToSQLType != NULL);

  /* snprintf replaces boost::lexical_cast<string>(...) for string conversions
   * due to dev library compatibility issues (may be changed back to boost libs if desired) */
  char string_buffer[MAX_BYTE_LENGTH];

  /* Fill in the values that we can below, leave the rest to default values */
  HiveColumnDesc column_desc;
  column_desc.initialize(m_columns[m_col_fetch_idx]);
  int column_num;
  for (unsigned int idx = 0; idx < LENGTH(g_columns_schema); idx++) {
    /* column_num represents ordinal position instead of index to avoid confusion */
    column_num = idx + 1;
    switch (column_num)
    {
    case 3: // If Col3: TABLE_NAME
      m_curr_row_data.push_back(m_tables[m_tbl_fetch_idx]);
      break;

    case 4: // If Col4: COLUMN_NAME
      m_curr_row_data.push_back(m_columns[m_col_fetch_idx].name);
      break;

    case 5: // If Col5: DATA_TYPE
      snprintf(string_buffer, sizeof(string_buffer), "%i",
               (*m_fpHiveToSQLType)(column_desc.getHiveType()));
      m_curr_row_data.push_back(string_buffer);
      break;

    case 6: // If Col6: TYPE_NAME
      m_curr_row_data.push_back(m_columns[m_col_fetch_idx].type);
      break;

    case 7: // If Col7: COLUMN_SIZE
      snprintf(string_buffer, sizeof(string_buffer), "%zu", column_desc.getMaxDisplaySize());
      m_curr_row_data.push_back(string_buffer);
      break;

    case 8: // If Col8: BUFFER_LENGTH
      snprintf(string_buffer, sizeof(string_buffer), "%zu", column_desc.getFieldByteSize());
      m_curr_row_data.push_back(string_buffer);
      break;

    case 11: // If Col11: NULLABLE
      /* Slight breakage in abstraction: should return SQL_NULLABLE and SQL_NO_NULLS,
       * but currently no means to access these values */
      m_curr_row_data.push_back((column_desc.getIsNullable() ? "1" : "0"));
      break;

    case 12: // If Col12: REMARKS
      m_curr_row_data.push_back(m_columns[m_col_fetch_idx].comment);
      break;

    case 14: // If Col14: SQL_DATA_TYPE
      snprintf(string_buffer, sizeof(string_buffer), "%i",
               (*m_fpHiveToSQLType)(column_desc.getHiveType()));
      m_curr_row_data.push_back(string_buffer);
      break;

    case 17: // If Col17: ORDINAL_POSITION
      snprintf(string_buffer, sizeof(string_buffer), "%i", m_col_fetch_idx + 1);
      m_curr_row_data.push_back(string_buffer);
      break;

    case 18: // If Col18: IS_NULLABLE
      m_curr_row_data.push_back((column_desc.getIsNullable() ? "YES" : "NO"));
      break;

    default: // Add in default value
      m_curr_row_data.push_back(g_columns_schema[idx][2]);
      break;
    }
  }

  /* There should be exactly 18 columns for this resultset */
  assert(m_curr_row_data.size() == 18);
  return HIVE_SUCCESS;
}

