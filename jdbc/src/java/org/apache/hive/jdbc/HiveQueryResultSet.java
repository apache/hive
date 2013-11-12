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

package org.apache.hive.jdbc;

import static org.apache.hive.service.cli.thrift.TCLIServiceConstants.TYPE_NAMES;

import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIServiceConstants;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.hive.service.cli.thrift.TTableSchema;
import org.apache.hive.service.cli.thrift.TTypeQualifierValue;
import org.apache.hive.service.cli.thrift.TTypeQualifiers;

/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Log LOG = LogFactory.getLog(HiveQueryResultSet.class);

  private TCLIService.Iface client;
  private TOperationHandle stmtHandle;
  private TSessionHandle sessHandle;
  private int maxRows;
  private int fetchSize;
  private int rowsFetched = 0;

  private List<TRow> fetchedRows;
  private Iterator<TRow> fetchedRowsItr;
  private boolean isClosed = false;
  private boolean emptyResultSet = false;

  public static class Builder {

    private final Statement statement;
    private TCLIService.Iface client = null;
    private TOperationHandle stmtHandle = null;
    private TSessionHandle sessHandle  = null;

    /**
     * Sets the limit for the maximum number of rows that any ResultSet object produced by this
     * Statement can contain to the given number. If the limit is exceeded, the excess rows
     * are silently dropped. The value must be >= 0, and 0 means there is not limit.
     */
    private int maxRows = 0;
    private boolean retrieveSchema = true;
    private List<String> colNames;
    private List<String> colTypes;
    private List<JdbcColumnAttributes> colAttributes;
    private int fetchSize = 50;
    private boolean emptyResultSet = false;

    public Builder(Statement statement) {
      this.statement = statement;
    }

    public Builder setClient(TCLIService.Iface client) {
      this.client = client;
      return this;
    }

    public Builder setStmtHandle(TOperationHandle stmtHandle) {
      this.stmtHandle = stmtHandle;
      return this;
    }

    public Builder setSessionHandle(TSessionHandle sessHandle) {
      this.sessHandle = sessHandle;
      return this;
    }

    public Builder setMaxRows(int maxRows) {
      this.maxRows = maxRows;
      return this;
    }

    public Builder setSchema(List<String> colNames, List<String> colTypes) {
      // no column attributes provided - create list of null attributes.
      List<JdbcColumnAttributes> colAttributes =
          new ArrayList<JdbcColumnAttributes>();
      for (int idx = 0; idx < colTypes.size(); ++idx) {
        colAttributes.add(null);
      }
      return setSchema(colNames, colTypes, colAttributes);
    }

    public Builder setSchema(List<String> colNames, List<String> colTypes,
        List<JdbcColumnAttributes> colAttributes) {
      this.colNames = new ArrayList<String>();
      this.colNames.addAll(colNames);
      this.colTypes = new ArrayList<String>();
      this.colTypes.addAll(colTypes);
      this.colAttributes = new ArrayList<JdbcColumnAttributes>();
      this.colAttributes.addAll(colAttributes);
      this.retrieveSchema = false;
      return this;
    }

    public Builder setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder setEmptyResultSet(boolean emptyResultSet) {
      this.emptyResultSet = emptyResultSet;
      return this;
    }

    public HiveQueryResultSet build() throws SQLException {
      return new HiveQueryResultSet(this);
    }
  }

  protected HiveQueryResultSet(Builder builder) throws SQLException {
    this.statement = builder.statement;
    this.client = builder.client;
    this.stmtHandle = builder.stmtHandle;
    this.sessHandle = builder.sessHandle;
    this.fetchSize = builder.fetchSize;
    columnNames = new ArrayList<String>();
    columnTypes = new ArrayList<String>();
    columnAttributes = new ArrayList<JdbcColumnAttributes>();
    if (builder.retrieveSchema) {
      retrieveSchema();
    } else {
      this.setSchema(builder.colNames, builder.colTypes, builder.colAttributes);
    }
    this.emptyResultSet = builder.emptyResultSet;
    if (builder.emptyResultSet) {
      this.maxRows = 0;
    } else {
      this.maxRows = builder.maxRows;
    }
  }

  /**
   * Generate ColumnAttributes object from a TTypeQualifiers
   * @param primitiveTypeEntry primitive type
   * @return generated ColumnAttributes, or null
   */
  private static JdbcColumnAttributes getColumnAttributes(
      TPrimitiveTypeEntry primitiveTypeEntry) {
    JdbcColumnAttributes ret = null;
    if (primitiveTypeEntry.isSetTypeQualifiers()) {
      TTypeQualifiers tq = primitiveTypeEntry.getTypeQualifiers();
      switch (primitiveTypeEntry.getType()) {
        case CHAR_TYPE:
        case VARCHAR_TYPE:
          TTypeQualifierValue val =
              tq.getQualifiers().get(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH);
          if (val != null) {
            // precision is char length
            ret = new JdbcColumnAttributes(val.getI32Value(), 0);
          }
          break;
        case DECIMAL_TYPE:
          TTypeQualifierValue prec = tq.getQualifiers().get(TCLIServiceConstants.PRECISION);
          TTypeQualifierValue scale = tq.getQualifiers().get(TCLIServiceConstants.SCALE);
          ret = new JdbcColumnAttributes(prec == null ? HiveDecimal.DEFAULT_PRECISION : prec.getI32Value(),
              scale == null ? HiveDecimal.DEFAULT_SCALE : scale.getI32Value());
          break;
        default:
          break;
      }
    }
    return ret;
  }

  /**
   * Retrieve schema from the server
   */
  private void retrieveSchema() throws SQLException {
    try {
      TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(stmtHandle);
      // TODO need session handle
      TGetResultSetMetadataResp  metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.verifySuccess(metadataResp.getStatus());

      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      TTableSchema schema = metadataResp.getSchema();
      if (schema == null || !schema.isSetColumns()) {
        // TODO: should probably throw an exception here.
        return;
      }
      setSchema(new TableSchema(schema));

      List<TColumnDesc> columns = schema.getColumns();
      for (int pos = 0; pos < schema.getColumnsSize(); pos++) {
        if (pos != 0) {
          namesSb.append(",");
          typesSb.append(",");
        }
        String columnName = columns.get(pos).getColumnName();
        columnNames.add(columnName);
        TPrimitiveTypeEntry primitiveTypeEntry =
            columns.get(pos).getTypeDesc().getTypes().get(0).getPrimitiveEntry();
        String columnTypeName = TYPE_NAMES.get(primitiveTypeEntry.getType());
        columnTypes.add(columnTypeName);
        columnAttributes.add(getColumnAttributes(primitiveTypeEntry));
      }
    } catch (SQLException eS) {
      throw eS; // rethrow the SQLException as is
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }

  /**
   * Set the specified schema to the resultset
   * @param colNames
   * @param colTypes
   */
  private void setSchema(List<String> colNames, List<String> colTypes,
      List<JdbcColumnAttributes> colAttributes) {
    columnNames.addAll(colNames);
    columnTypes.addAll(colTypes);
    columnAttributes.addAll(colAttributes);
  }

  @Override
  public void close() throws SQLException {
    if (this.statement != null && (this.statement instanceof HiveStatement)) {
      HiveStatement s = (HiveStatement) this.statement;
      s.closeClientOperation();
    }
    // Need reset during re-open when needed
    client = null;
    stmtHandle = null;
    sessHandle = null;
    isClosed = true;
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  public boolean next() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (emptyResultSet || (maxRows > 0 && rowsFetched >= maxRows)) {
      return false;
    }

    try {
      if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle,
            TFetchOrientation.FETCH_NEXT, fetchSize);
        TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
        Utils.verifySuccessWithInfo(fetchResp.getStatus());
        fetchedRows = fetchResp.getResults().getRows();
        fetchedRowsItr = fetchedRows.iterator();
      }

      String rowStr = "";
      if (fetchedRowsItr.hasNext()) {
        row = fetchedRowsItr.next();
      } else {
        return false;
      }

      rowsFetched++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched row string: " + rowStr);
      }

    } catch (SQLException eS) {
      throw eS;
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row", ex);
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return true;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return super.getMetaData();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return fetchSize;
  }

  public <T> T getObject(String columnLabel, Class<T> type)  throws SQLException {
    //JDK 1.7
    throw new SQLException("Method not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type)  throws SQLException {
    //JDK 1.7
    throw new SQLException("Method not supported");
  }
}
