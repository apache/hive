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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * GetTablesOperation.
 *
 */
public class GetTablesOperation extends MetadataOperation {

  private final String catalogName;
  private final String schemaName;
  private final String tableName;
  private final List<String> tableTypes = new ArrayList<String>();
  private final RowSet rowSet = new RowSet();


  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addStringColumn("TABLE_CAT", "Catalog name. NULL if not applicable.")
  .addStringColumn("TABLE_SCHEMA", "Schema name.")
  .addStringColumn("TABLE_NAME", "Table name.")
  .addStringColumn("TABLE_TYPE", "The table type, e.g. \"TABLE\", \"VIEW\", etc.")
  .addStringColumn("REMARKS", "Comments about the table.");

  protected GetTablesOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName,
      List<String> tableTypes) {
    super(parentSession, OperationType.GET_TABLES);
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    if (tableTypes != null) {
      this.tableTypes.addAll(tableTypes);
    }
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
      for (String dbName : metastoreClient.getDatabases(schemaPattern)) {
        List<String> tableNames = metastoreClient.getTables(dbName, tablePattern);
        for (Table table : metastoreClient.getTableObjectsByName(dbName, tableNames)) {
          Object[] rowData = new Object[] {
              DEFAULT_HIVE_CATALOG,
              table.getDbName(),
              table.getTableName(),
              table.getTableType(),
              table.getParameters().get("comment")
              };
          if (tableTypes.isEmpty() || tableTypes.contains(table.getTableType())) {
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
