/*
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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.NumberUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * GetPrimaryKeysOperation.
 *
 */
public class GetPrimaryKeysOperation extends MetadataOperation {
/**
TABLE_CAT String => table catalog (may be null)
TABLE_SCHEM String => table schema (may be null)
TABLE_NAME String => table name
COLUMN_NAME String => column name
KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
PK_NAME String => primary key name (may be null)
 */
  private static final TableSchema RESULT_SET_SCHEMA = new TableSchema()
  .addPrimitiveColumn("TABLE_CAT", Type.STRING_TYPE,
      "Table catalog (may be null)")
  .addPrimitiveColumn("TABLE_SCHEM", Type.STRING_TYPE,
      "Table schema (may be null)")
  .addPrimitiveColumn("TABLE_NAME", Type.STRING_TYPE,
      "Table name")
  .addPrimitiveColumn("COLUMN_NAME", Type.STRING_TYPE,
      "Column name")
  .addPrimitiveColumn("KEQ_SEQ", Type.INT_TYPE,
      "Sequence number within primary key")
  .addPrimitiveColumn("PK_NAME", Type.STRING_TYPE,
      "Primary key name (may be null)");

  private final String catalogName;
  private final String schemaName;
  private final String tableName;

  private final RowSet rowSet;

  public GetPrimaryKeysOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName) {
    super(parentSession, OperationType.GET_FUNCTIONS);
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion(), false);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      IMetaStoreClient metastoreClient = getParentSession().getMetaStoreClient();
      PrimaryKeysRequest sqlReq = new PrimaryKeysRequest(schemaName, tableName);
      List<SQLPrimaryKey> pks = metastoreClient.getPrimaryKeys(sqlReq);
      if (pks == null) {
        return;
      }
      for (SQLPrimaryKey pk : pks) {
	    rowSet.addRow(new Object[] {catalogName, pk.getTable_db(), 
	      pk.getTable_name(), pk.getColumn_name(), pk.getKey_seq(), pk.getPk_name()});
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
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    return RESULT_SET_SCHEMA;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    assertState(new ArrayList<OperationState>(Arrays.asList(OperationState.FINISHED)));
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0);
    }
    return rowSet.extractSubset((int)maxRows);
  }
}
