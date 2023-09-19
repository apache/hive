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

package org.apache.hive.jdbc;

import static org.apache.hive.service.rpc.thrift.TCLIServiceConstants.TYPE_NAMES;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIServiceConstants;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;
import org.apache.hive.service.rpc.thrift.TTypeQualifiers;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Logger LOG = LoggerFactory.getLogger(HiveQueryResultSet.class);

  private Connection connection;
  private TCLIService.Iface client;
  private TOperationHandle stmtHandle;
  private TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT;
  private boolean checkOperationStatus;
  private int maxRows;
  private int fetchSize;
  private long rowsFetched = 0;
  private boolean fetchDone = false;

  private RowSet fetchedRows;
  private Iterator<Object[]> fetchedRowsItr;
  private boolean isClosed = false;
  private boolean emptyResultSet = false;
  private boolean isScrollable = false;

  private final TProtocolVersion protocol;

  ExecutorService pool = null;
  AtomicBoolean hasStartRow = new AtomicBoolean(false);
  int fetchThreads = 1;
  int threadsStarted = 0;

  private class FetchResult {
    Exception ex;
    RowSet fetchedRows;
    boolean hasMoreRows;
    long startRow;
    int numRows;
  }

  BlockingQueue<FetchResult> resultQueue;
  AtomicLong nextStartRow = new AtomicLong(1L);
  AtomicReference<InterruptedException> interruptException = new AtomicReference<>();
  AtomicBoolean gotLastBatch = new AtomicBoolean(false);
  AtomicBoolean poolDone = new AtomicBoolean(false);

  public static class Builder {

    private final Statement statement;
    private Connection connection = null;
    private TCLIService.Iface client = null;
    private TOperationHandle stmtHandle = null;

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
    private int fetchThreads = 1;
    private boolean emptyResultSet = false;
    private boolean isScrollable = false;

    public Builder(Statement statement) throws SQLException {
      this.statement = statement;
      this.connection = statement.getConnection();
    }

    public Builder(Connection connection) {
      this.statement = null;
      this.connection = connection;
    }

    public Builder setConnection(Connection connection) {
      this.connection = connection;
      return this;
    }

    public Builder setClient(TCLIService.Iface client) {
      this.client = client;
      return this;
    }

    public Builder setStmtHandle(TOperationHandle stmtHandle) {
      this.stmtHandle = stmtHandle;
      return this;
    }

    public Builder setMaxRows(int maxRows) {
      this.maxRows = maxRows;
      return this;
    }

    public Builder setSchema(List<String> colNames, List<String> colTypes) {
      // no column attributes provided - create list of null attributes.
      List<JdbcColumnAttributes> colAttributes =
          Collections.nCopies(colTypes.size(), null);
      return setSchema(colNames, colTypes, colAttributes);
    }

    public Builder setSchema(List<String> colNames, List<String> colTypes,
        List<JdbcColumnAttributes> colAttributes) {
      this.colNames = new ArrayList<>(colNames);
      this.colTypes = new ArrayList<>(colTypes);
      this.colAttributes = new ArrayList<>(colAttributes);
      this.retrieveSchema = false;
      return this;
    }

    public Builder setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder setFetchThreads(int fetchThreads) {
      this.fetchThreads = fetchThreads;
      return this;
    }

    public Builder setEmptyResultSet(boolean emptyResultSet) {
      this.emptyResultSet = emptyResultSet;
      return this;
    }

    public Builder setScrollable(boolean setScrollable) {
      this.isScrollable = setScrollable;
      return this;
    }

    public HiveQueryResultSet build() throws SQLException {
      return new HiveQueryResultSet(this);
    }

    public TProtocolVersion getProtocolVersion() throws SQLException {
      return ((HiveConnection)connection).getProtocol();
    }
  }

  protected HiveQueryResultSet(Builder builder) throws SQLException {
    this.statement = builder.statement;
    this.client = builder.client;
    this.connection = builder.connection;
    this.stmtHandle = builder.stmtHandle;
    this.fetchSize = builder.fetchSize;
    this.fetchThreads = builder.fetchThreads;
    columnNames = new ArrayList<String>();
    normalizedColumnNames = new ArrayList<String>();
    columnTypes = new ArrayList<String>();
    columnAttributes = new ArrayList<JdbcColumnAttributes>();
    if (builder.retrieveSchema) {
      retrieveSchema();
    } else {
      this.setSchema(builder.colNames, builder.colTypes, builder.colAttributes);
    }
    this.emptyResultSet = builder.emptyResultSet;
    this.maxRows = builder.maxRows;
    checkOperationStatus = (statement instanceof HiveStatement);
    this.isScrollable = builder.isScrollable;
    this.protocol = builder.getProtocolVersion();
    initEmptyIterator();
    resultQueue = new ArrayBlockingQueue<>(Math.max(fetchThreads, 1));
    // The fetch path is usually the bottleneck with a single-threaded fetch
    // so only convert results there if there are multiple fetch threads.
    if (fetchThreads > 1) {
      useConvertedResultSet = true;
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
          ret = new JdbcColumnAttributes(prec == null ? HiveDecimal.USER_DEFAULT_PRECISION : prec.getI32Value(),
              scale == null ? HiveDecimal.USER_DEFAULT_SCALE : scale.getI32Value());
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
      TGetResultSetMetadataResp  metadataResp;
      metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.verifySuccess(metadataResp.getStatus());

      TTableSchema schema = metadataResp.getSchema();
      if (schema == null || !schema.isSetColumns()) {
        // TODO: should probably throw an exception here.
        return;
      }
      setSchema(new TableSchema(schema));

      for (final TColumnDesc column : schema.getColumns()) {
        String columnName = column.getColumnName();
        columnNames.add(columnName);
        normalizedColumnNames.add(columnName.toLowerCase());
        TPrimitiveTypeEntry primitiveTypeEntry =
            column.getTypeDesc().getTypes().get(0).getPrimitiveEntry();
        String columnTypeName = TYPE_NAMES.get(primitiveTypeEntry.getType());
        columnTypes.add(columnTypeName);
        columnAttributes.add(getColumnAttributes(primitiveTypeEntry));
      }
    } catch (SQLException eS) {
      throw eS; // rethrow the SQLException as is
    } catch (Exception ex) {
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

    colNames.forEach(i -> normalizedColumnNames.add(i.toLowerCase()));
  }

  private void initEmptyIterator() throws SQLException {
    try {
      fetchedRows = RowSetFactory.create(new TRowSet(), protocol);
      fetchedRowsItr = fetchedRows.iterator();
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    shutdownPool();
    if (this.statement != null && (this.statement instanceof HiveStatement)) {
      /*
       * HIVE-25203: Be aware that a ResultSet is not supposed to control its parent Statement's
       * lifecycle, so before adding any logic into this branch, make sure if it's really correct.
       * One known exception is Statement#closeOnCompletion, which is part of the Statement API.
       */
      HiveStatement s = (HiveStatement) this.statement;
      s.closeOnResultSetCompletion();
    } else {
      // for those stmtHandle passed from HiveDatabaseMetaData instead of Statement
      closeOperationHandle(stmtHandle);
    }

    // Need reset during re-open when needed
    client = null;
    stmtHandle = null;
    isClosed = true;
    initEmptyIterator();
  }

  private void closeOperationHandle(TOperationHandle stmtHandle) throws SQLException {
    try {
      if (stmtHandle != null) {
        TCloseOperationReq closeReq = new TCloseOperationReq(stmtHandle);
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        Utils.verifySuccessWithInfo(closeResp.getStatus());
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e.toString(), "08S01", e);
    }
  }

  private void closeConn(HiveConnection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOG.debug("Error closing connection {}", e.toString());
      }
    }
  }

  private boolean nextRowBatch() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if ((maxRows > 0 && rowsFetched >= maxRows) || emptyResultSet || fetchDone) {
      return false;
    }
    if (checkOperationStatus) {
      ((HiveStatement) statement).waitForOperationToComplete();
      checkOperationStatus = false;
    }

// TODO: Could support pool with maxRows by bounding results instead
    if (rowsFetched < fetchSize || fetchThreads == 0 || maxRows > 0) {
      try {
        int fetchSizeBounded = fetchSize;
        if (maxRows > 0 && rowsFetched + fetchSize > maxRows) {
          fetchSizeBounded = maxRows - (int)rowsFetched;
        }
        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle,
            orientation, fetchSizeBounded);
        TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
        Utils.verifySuccessWithInfo(fetchResp.getStatus());
        TRowSet results = fetchResp.getResults();
        if (results.getStartRowOffset() > 0) {
          hasStartRow.set(true);
        }
        fetchedRows = RowSetFactory.create(results, protocol);
        fetchDone = !fetchResp.isHasMoreRows() && fetchedRows.numRows() == 0;
        if (fetchDone) {
          gotLastBatch.set(true);
        }
        fetchedRows = RowSetFactory.create(results, protocol);
        if (useConvertedResultSet) {
          fetchedRows = new ConvertedResultSet(fetchedRows, getSchema());
        }

        nextStartRow.set(results.getStartRowOffset() + 1 + fetchedRows.numRows());
      } catch (TException ex) {
        throw new SQLException("Error retrieving next row", ex);
      }
    } else {
      if (!gotLastBatch.get()) {
        if (pool == null) {
          pool = Executors.newFixedThreadPool(fetchThreads);
        }
        // Add another thread on each row batch up to the limit
        if (threadsStarted < (hasStartRow.get() ? fetchThreads : 1)) {
          final boolean useMainClient = (threadsStarted == 0);
          threadsStarted++;
          pool.execute(()-> {
            LOG.debug("Started thread {}", Thread.currentThread().getName());

            TCLIService.Iface fetchClient = null;
            HiveConnection threadConn = null;
            long startTime = System.nanoTime();

            while (!gotLastBatch.get() && !poolDone.get()) {
              if (threadConn != connection) {
                long endTime = System.nanoTime();
                // Re-open cloned connections every 5 sec to avoid starvation
                if (endTime - startTime > 5000000000L) {
                  closeConn(threadConn);
                  threadConn = null;
                  startTime = endTime;
                }
              }
              if (threadConn == null) {
                if (useMainClient) {
                  threadConn = (HiveConnection)connection;
                  fetchClient = client;
                } else {
                  try {
                    threadConn = new HiveConnection((HiveConnection)connection);
                    fetchClient = threadConn.getClient();
                  } catch (SQLException e) {
                    LOG.debug("Multi-stream connection error {}", e.toString());
                    return;
                  }
                }
              }
              FetchResult result = new FetchResult();
              try {
                final TFetchResultsReq fetchReq = new TFetchResultsReq(
                    stmtHandle, orientation, fetchSize);
                TFetchResultsResp fetchResp = fetchClient.FetchResults(fetchReq);
                Utils.verifySuccessWithInfo(fetchResp.getStatus());
                TRowSet results = fetchResp.getResults();
                if (results.getStartRowOffset() > 0) {
                  hasStartRow.set(true);
                }
                result.fetchedRows = RowSetFactory.create(results, protocol);
                result.numRows = result.fetchedRows.numRows();
                boolean hasMoreRows = result.numRows > 0 || fetchResp.isHasMoreRows();
                if (!hasMoreRows) {
                  gotLastBatch.set(true);
                }
                result.hasMoreRows = hasMoreRows;
                result.fetchedRows = RowSetFactory.create(results, protocol);
                if (useConvertedResultSet) {
                  result.fetchedRows =
                      new ConvertedResultSet(result.fetchedRows, getSchema());
                }
                result.startRow = results.getStartRowOffset() + 1;
                if (hasStartRow.get() && result.startRow < nextStartRow.get()) {
                  throw new SQLException("Unexpected row offset");
                }
              } catch (Exception e) {
                result.ex = e;
              }

              try {
                // Wait for earlier row sets to be added to the queue
                synchronized(nextStartRow) {
                  if (!poolDone.get()) {
                    if (result.ex == null) {
                      if (hasStartRow.get()) {
                        while (nextStartRow.get() != result.startRow) {
                          nextStartRow.wait();
                        }
                        nextStartRow.set(result.startRow + result.numRows);
                      }
                      poolDone.set(!result.hasMoreRows);
                    } else {
                      poolDone.set(true);
                    }
                    resultQueue.put(result);
                    if (hasStartRow.get()) {
                      nextStartRow.notifyAll();
                    }
                  }
                }
              } catch (InterruptedException e) {
                interruptException.set(e);
                break;
              }
            }
            if (threadConn != connection) {
              closeConn(threadConn);
            }
          });
        }
      }
      try {
        if (interruptException.get() != null) {
          throw interruptException.get();
        }
        FetchResult result = resultQueue.take();
        fetchDone = !result.hasMoreRows;
        if (result.ex != null) {
          shutdownPool();
          throw new SQLException(result.ex);
        }
        fetchedRows = result.fetchedRows;
      } catch (InterruptedException e) {
        throw new SQLException(e);
      }
    }

    orientation = TFetchOrientation.FETCH_NEXT;
    fetchedRowsItr = fetchedRows.iterator();

    return fetchedRowsItr.hasNext();
  }

  void drainQueue() throws SQLException {
    FetchResult result;
    while ((result = resultQueue.poll()) != null) {
      if (result.ex != null) {
        throw new SQLException(result.ex);
      }
    }
  }

  void shutdownPool() throws SQLException {
    if (pool != null) {
      poolDone.set(true);
      drainQueue();
      pool.shutdownNow();
      try {
        while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
          drainQueue();
          LOG.debug("Slow fetch thread shutdown");
        }
      } catch (InterruptedException e) {
        throw new SQLException(e);
      }
      drainQueue();
      pool = null;
      threadsStarted = 0;
    }
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  public boolean next() throws SQLException {
    while (!fetchedRowsItr.hasNext()) {
      if (!nextRowBatch()) {
        return false;
      }
    }
    row = fetchedRowsItr.next();
    rowsFetched++;
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
  public int getType() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (isScrollable) {
      return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }
    return ResultSet.TYPE_FORWARD_ONLY;
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
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public <T> T getObject(int columnIndex, Class<T> type)  throws SQLException {
    //JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /**
   * Moves the cursor before the first row of the resultset.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  @Override
  public void beforeFirst() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (!isScrollable) {
      throw new SQLException("Method not supported for TYPE_FORWARD_ONLY resultset");
    }

    // If we are asked to start from begining, clear the current fetched resultset
    shutdownPool();
    initEmptyIterator();
    orientation = TFetchOrientation.FETCH_FIRST;
    rowsFetched = 0;
    nextStartRow.set(1L);
    fetchDone = false;
    poolDone.set(false);
    gotLastBatch.set(false);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return (rowsFetched == 0);
  }

  @Override
  public int getRow() throws SQLException {
    if (rowsFetched > Integer.MAX_VALUE) {
      throw new SQLException("getRow() result exceeds Int.MAX_VALUE");
    }
    return (int)rowsFetched;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
