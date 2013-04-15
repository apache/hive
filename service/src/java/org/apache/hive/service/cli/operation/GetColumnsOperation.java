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

package org.apache.hive.service.cli.operation;

import java.sql.DatabaseMetaData;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * GetColumnsOperation.
 *
 */
public class GetColumnsOperation extends MetadataOperation {

  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addPrimitiveColumn("TABLE_CAT", Type.STRING_TYPE,
      "Catalog name. NULL if not applicable")
  .addPrimitiveColumn("TABLE_SCHEM", Type.STRING_TYPE,
      "Schema name")
  .addPrimitiveColumn("TABLE_NAME", Type.STRING_TYPE,
      "Table name")
  .addPrimitiveColumn("COLUMN_NAME", Type.STRING_TYPE,
      "Column name")
  .addPrimitiveColumn("DATA_TYPE", Type.INT_TYPE,
      "SQL type from java.sql.Types")
  .addPrimitiveColumn("TYPE_NAME", Type.STRING_TYPE,
      "Data source dependent type name, for a UDT the type name is fully qualified")
  .addPrimitiveColumn("COLUMN_SIZE", Type.INT_TYPE,
      "Column size. For char or date types this is the maximum number of characters,"
      + " for numeric or decimal types this is precision.")
  .addPrimitiveColumn("BUFFER_LENGTH", Type.TINYINT_TYPE,
      "Unused")
  .addPrimitiveColumn("DECIMAL_DIGITS", Type.INT_TYPE,
      "The number of fractional digits")
  .addPrimitiveColumn("NUM_PREC_RADIX", Type.INT_TYPE,
      "Radix (typically either 10 or 2)")
  .addPrimitiveColumn("NULLABLE", Type.INT_TYPE,
      "Is NULL allowed")
  .addPrimitiveColumn("REMARKS", Type.STRING_TYPE,
      "Comment describing column (may be null)")
  .addPrimitiveColumn("COLUMN_DEF", Type.STRING_TYPE,
      "Default value (may be null)")
  .addPrimitiveColumn("SQL_DATA_TYPE", Type.INT_TYPE,
      "Unused")
  .addPrimitiveColumn("SQL_DATETIME_SUB", Type.INT_TYPE,
      "Unused")
  .addPrimitiveColumn("CHAR_OCTET_LENGTH", Type.INT_TYPE,
      "For char types the maximum number of bytes in the column")
  .addPrimitiveColumn("ORDINAL_POSITION", Type.INT_TYPE,
      "Index of column in table (starting at 1)")
  .addPrimitiveColumn("IS_NULLABLE", Type.STRING_TYPE,
      "\"NO\" means column definitely does not allow NULL values; "
      + "\"YES\" means the column might allow NULL values. An empty "
      + "string means nobody knows.")
  .addPrimitiveColumn("SCOPE_CATALOG", Type.STRING_TYPE,
      "Catalog of table that is the scope of a reference attribute "
      + "(null if DATA_TYPE isn't REF)")
  .addPrimitiveColumn("SCOPE_SCHEMA", Type.STRING_TYPE,
      "Schema of table that is the scope of a reference attribute "
      + "(null if the DATA_TYPE isn't REF)")
  .addPrimitiveColumn("SCOPE_TABLE", Type.STRING_TYPE,
      "Table name that this the scope of a reference attribure "
      + "(null if the DATA_TYPE isn't REF)")
  .addPrimitiveColumn("SOURCE_DATA_TYPE", Type.SMALLINT_TYPE,
      "Source type of a distinct type or user-generated Ref type, "
      + "SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)")
  .addPrimitiveColumn("IS_AUTO_INCREMENT", Type.STRING_TYPE,
      "Indicates whether this column is auto incremented.");

  private final String catalogName;
  private final String schemaName;
  private final String tableName;
  private final String columnName;

  private final RowSet rowSet = new RowSet();

  protected GetColumnsOperation(HiveSession parentSession, String catalogName, String schemaName,
      String tableName, String columnName) {
    super(parentSession, OperationType.GET_COLUMNS);
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#run()
   */
  @Override
  public void run() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      IMetaStoreClient metastoreClient = getParentSession().getMetaStoreClient();
      String schemaPattern = convertSchemaPattern(schemaName);
      String tablePattern = convertIdentifierPattern(tableName, true);

      Pattern columnPattern = null;
      if (columnName != null) {
        columnPattern = Pattern.compile(convertIdentifierPattern(columnName, false));
      }

      List<String> dbNames = metastoreClient.getDatabases(schemaPattern);
      Collections.sort(dbNames);
      for (String dbName : dbNames) {
        List<String> tableNames = metastoreClient.getTables(dbName, tablePattern);
        Collections.sort(tableNames);
        for (Table table : metastoreClient.getTableObjectsByName(dbName, tableNames)) {
          TableSchema schema = new TableSchema(metastoreClient.getSchema(dbName, table.getTableName()));
          for (ColumnDescriptor column : schema.getColumnDescriptors()) {
            if (columnPattern != null && !columnPattern.matcher(column.getName()).matches()) {
              continue;
            }
            Object[] rowData = new Object[] {
                null,  // TABLE_CAT
                table.getDbName(), // TABLE_SCHEM
                table.getTableName(), // TABLE_NAME
                column.getName(), // COLUMN_NAME
                column.getType().toJavaSQLType(), // DATA_TYPE
                column.getTypeName(), // TYPE_NAME
                column.getType().getColumnSize(), // COLUMN_SIZE
                null, // BUFFER_LENGTH, unused
                column.getType().getDecimalDigits(), // DECIMAL_DIGITS
                column.getType().getNumPrecRadix(), // NUM_PREC_RADIX
                DatabaseMetaData.columnNullable, // NULLABLE
                column.getComment(), // REMARKS
                null, // COLUMN_DEF
                null, // SQL_DATA_TYPE
                null, // SQL_DATETIME_SUB
                null, // CHAR_OCTET_LENGTH
                column.getOrdinalPosition(), // ORDINAL_POSITION
                "YES", // IS_NULLABLE
                null, // SCOPE_CATALOG
                null, // SCOPE_SCHEMA
                null, // SCOPE_TABLE
                null, // SOURCE_DATA_TYPE
                "NO", // IS_AUTO_INCREMENT
            };
            rowSet.addRow(RESULT_SET_SCHEMA, rowData);
          }
        }
      }
      setState(OperationState.FINISHED);
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException(e);
    }

  }


  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(OperationState.FINISHED);
    return rowSet.extractSubset((int)maxRows);
  }

}
