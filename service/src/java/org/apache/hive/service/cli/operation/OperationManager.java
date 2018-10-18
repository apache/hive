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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.log.LogDivertAppender;
import org.apache.hadoop.hive.ql.log.LogDivertAppenderForTest;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OperationManager.
 *
 */
public class OperationManager extends AbstractService {
  private final Logger LOG = LoggerFactory.getLogger(OperationManager.class.getName());
  private final ConcurrentHashMap<OperationHandle, Operation> handleToOperation =
      new ConcurrentHashMap<OperationHandle, Operation>();
  private final ConcurrentHashMap<String, Operation> queryIdOperation =
      new ConcurrentHashMap<String, Operation>();
  private final ConcurrentHashMap<String, String> queryTagToIdMap = new ConcurrentHashMap<>();

  //Following fields for displaying queries on WebUI
  private Object webuiLock = new Object();
  private QueryInfoCache historicalQueryInfos;
  private Map<String, QueryInfo> liveQueryInfos = new LinkedHashMap<>();

  public OperationManager() {
    super(OperationManager.class.getSimpleName());
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    LogDivertAppender.registerRoutingAppender(hiveConf);
    LogDivertAppenderForTest.registerRoutingAppenderIfInTest(hiveConf);

    if (hiveConf.isWebUiQueryInfoCacheEnabled()) {
      historicalQueryInfos = new QueryInfoCache(
        hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_MAX_HISTORIC_QUERIES));
    }
    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  public ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession,
      String statement, Map<String, String> confOverlay, boolean runAsync, long queryTimeout)
      throws HiveSQLException {
    ExecuteStatementOperation executeStatementOperation =
        ExecuteStatementOperation.newExecuteStatementOperation(parentSession, statement,
            confOverlay, runAsync, queryTimeout);
    addOperation(executeStatementOperation);
    return executeStatementOperation;
  }

  public GetTypeInfoOperation newGetTypeInfoOperation(HiveSession parentSession) {
    GetTypeInfoOperation operation = new GetTypeInfoOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetCatalogsOperation newGetCatalogsOperation(HiveSession parentSession) {
    GetCatalogsOperation operation = new GetCatalogsOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetSchemasOperation newGetSchemasOperation(HiveSession parentSession,
      String catalogName, String schemaName) {
    GetSchemasOperation operation = new GetSchemasOperation(parentSession, catalogName, schemaName);
    addOperation(operation);
    return operation;
  }

  public MetadataOperation newGetTablesOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName,
      List<String> tableTypes) {
    MetadataOperation operation =
        new GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes);
    addOperation(operation);
    return operation;
  }

  public GetTableTypesOperation newGetTableTypesOperation(HiveSession parentSession) {
    GetTableTypesOperation operation = new GetTableTypesOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetColumnsOperation newGetColumnsOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName, String columnName) {
    GetColumnsOperation operation = new GetColumnsOperation(parentSession,
        catalogName, schemaName, tableName, columnName);
    addOperation(operation);
    return operation;
  }

  public GetFunctionsOperation newGetFunctionsOperation(HiveSession parentSession,
      String catalogName, String schemaName, String functionName) {
    GetFunctionsOperation operation = new GetFunctionsOperation(parentSession,
        catalogName, schemaName, functionName);
    addOperation(operation);
    return operation;
  }

  public GetPrimaryKeysOperation newGetPrimaryKeysOperation(HiveSession parentSession,
	      String catalogName, String schemaName, String tableName) {
    GetPrimaryKeysOperation operation = new GetPrimaryKeysOperation(parentSession,
	    catalogName, schemaName, tableName);
	addOperation(operation);
	return operation;
  }

  public GetCrossReferenceOperation newGetCrossReferenceOperation(
   HiveSession session, String primaryCatalog, String primarySchema,
   String primaryTable, String foreignCatalog, String foreignSchema,
   String foreignTable) {
   GetCrossReferenceOperation operation = new GetCrossReferenceOperation(session,
     primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema,
     foreignTable);
   addOperation(operation);
   return operation;
  }

  public Operation getOperation(OperationHandle operationHandle) throws HiveSQLException {
    Operation operation = getOperationInternal(operationHandle);
    if (operation == null) {
      throw new HiveSQLException("Invalid OperationHandle: " + operationHandle);
    }
    return operation;
  }

  private Operation getOperationInternal(OperationHandle operationHandle) {
    return handleToOperation.get(operationHandle);
  }

  private String getQueryId(Operation operation) {
    return operation.getParentSession().getHiveConf().getVar(ConfVars.HIVEQUERYID);
  }

  private void addOperation(Operation operation) {
    LOG.info("Adding operation: " + operation.getHandle());
    queryIdOperation.put(getQueryId(operation), operation);
    handleToOperation.put(operation.getHandle(), operation);
    if (operation instanceof SQLOperation) {
      synchronized (webuiLock) {
        liveQueryInfos.put(operation.getHandle().getHandleIdentifier().toString(),
          ((SQLOperation) operation).getQueryInfo());
      }
    }
  }

  public void updateQueryTag(String queryId, String queryTag) {
    Operation operation = queryIdOperation.get(queryId);
    if (operation != null) {
      String queryIdTemp = queryTagToIdMap.get(queryTag);
      if (queryIdTemp != null) {
        throw new RuntimeException("tag " + queryTag + " is already applied for query " + queryIdTemp);
      }
      queryTagToIdMap.put(queryTag, queryId);
      LOG.info("Query " + queryId + " is updated with tag " + queryTag);
      return;
    }
    LOG.info("Query id is missing during query tag updation");
  }

  private Operation removeOperation(OperationHandle opHandle) {
    Operation operation = handleToOperation.remove(opHandle);
    if (operation == null) {
      throw new RuntimeException("Operation does not exist: " + opHandle);
    }
    String queryId = getQueryId(operation);
    queryIdOperation.remove(queryId);
    String queryTag = operation.getQueryTag();
    if (queryTag != null) {
      queryTagToIdMap.remove(queryTag);
    }
    LOG.info("Removed queryId: {} corresponding to operation: {} with tag: {}", queryId, opHandle, queryTag);
    if (operation instanceof SQLOperation) {
      removeSafeQueryInfo(opHandle);
    }
    return operation;
  }

  private Operation removeTimedOutOperation(OperationHandle operationHandle) {
    Operation operation = handleToOperation.get(operationHandle);
    if (operation != null && operation.isTimedOut(System.currentTimeMillis())) {
      LOG.info("Operation is timed out,operation=" + operation.getHandle() + ",state=" + operation.getState().toString());
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        try {
          metrics.decrementCounter(MetricsConstant.OPEN_OPERATIONS);
        } catch (Exception e) {
          LOG.warn("Error decrementing open_operations metric, reported values may be incorrect", e);
        }
      }

      return removeOperation(operationHandle);
    }
    return null;
  }

  private void removeSafeQueryInfo(OperationHandle operationHandle) {
    synchronized (webuiLock) {
      String opKey = operationHandle.getHandleIdentifier().toString();
      // remove from list of live operations
      QueryInfo display = liveQueryInfos.remove(opKey);
      if (display == null) {
        LOG.debug("Unexpected display object value of null for operation {}",
            opKey);
      } else if (historicalQueryInfos != null) {
        // add to list of saved historic operations
        historicalQueryInfos.put(opKey, display);
      }
    }
  }

  public OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
    return getOperation(opHandle).getStatus();
  }

  /**
   * Cancel the running operation unless it is already in a terminal state
   * @param opHandle operation handle
   * @param errMsg error message
   * @throws HiveSQLException
   */
  public void cancelOperation(OperationHandle opHandle, String errMsg) throws HiveSQLException {
    Operation operation = getOperation(opHandle);
    OperationState opState = operation.getStatus().getState();
    if (opState.isTerminal()) {
      // Cancel should be a no-op in either cases
      LOG.debug(opHandle + ": Operation is already aborted in state - " + opState);
    } else {
      LOG.debug(opHandle + ": Attempting to cancel from state - " + opState);
      OperationState operationState = OperationState.CANCELED;
      operationState.setErrorMessage(errMsg);
      operation.cancel(operationState);
      if (operation instanceof SQLOperation) {
        removeSafeQueryInfo(opHandle);
      }
    }
  }

  /**
   * Cancel the running operation unless it is already in a terminal state
   * @param opHandle
   * @throws HiveSQLException
   */
  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    cancelOperation(opHandle, "");
  }

  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    LOG.info("Closing operation: " + opHandle);
    Operation operation = removeOperation(opHandle);
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      try {
        metrics.decrementCounter(MetricsConstant.OPEN_OPERATIONS);
      } catch (Exception e) {
        LOG.warn("Error Reporting close operation to Metrics system", e);
      }
    }
    operation.close();
  }

  public TableSchema getOperationResultSetSchema(OperationHandle opHandle)
      throws HiveSQLException {
    return getOperation(opHandle).getResultSetSchema();
  }

  public RowSet getOperationNextRowSet(OperationHandle opHandle,
      FetchOrientation orientation, long maxRows) throws HiveSQLException {
    return getOperation(opHandle).getNextRowSet(orientation, maxRows);
  }

  public RowSet getOperationLogRowSet(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, HiveConf hConf) throws HiveSQLException {
    TableSchema tableSchema = new TableSchema(getLogSchema());
    RowSet rowSet =
        RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion(), false);

    if (hConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED) == false) {
      LOG.warn("Try to get operation log when hive.server2.logging.operation.enabled is false, no log will be returned. ");
      return rowSet;
    }
    // get the OperationLog object from the operation
    OperationLog operationLog = getOperation(opHandle).getOperationLog();
    if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated with operation handle: " + opHandle);
    }

    // read logs
    List<String> logs;
    try {
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows);
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getCause());
    }

    // convert logs to RowSet
    for (String log : logs) {
      rowSet.addRow(new String[] { log });
    }

    return rowSet;
  }

  private boolean isFetchFirst(FetchOrientation fetchOrientation) {
    //TODO: Since OperationLog is moved to package o.a.h.h.ql.session,
    // we may add a Enum there and map FetchOrientation to it.
    if (fetchOrientation.equals(FetchOrientation.FETCH_FIRST)) {
      return true;
    }
    return false;
  }

  private Schema getLogSchema() {
    Schema schema = new Schema();
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setName("operation_log");
    fieldSchema.setType("string");
    schema.addToFieldSchemas(fieldSchema);
    return schema;
  }

  public Collection<Operation> getOperations() {
    return Collections.unmodifiableCollection(handleToOperation.values());
  }

  public List<Operation> removeExpiredOperations(OperationHandle[] handles) {
    List<Operation> removed = new ArrayList<Operation>();
    for (OperationHandle handle : handles) {
      Operation operation = removeTimedOutOperation(handle);
      if (operation != null) {
        LOG.warn("Operation " + handle + " is timed-out and will be closed");
        removed.add(operation);
      }
    }
    return removed;
  }

  /**
   * @return displays representing a number of historical SQLOperations, at max number of
   * hive.server2.webui.max.historic.queries. Newest items will be first.
   */
  public List<QueryInfo> getHistoricalQueryInfos() {
    List<QueryInfo> result = new LinkedList<>();
    synchronized (webuiLock) {
      if (historicalQueryInfos != null) {
        result.addAll(historicalQueryInfos.values());
        Collections.reverse(result);
      }
    }
    return result;
  }

  /**
   * @return displays representing live SQLOperations
   */
  public List<QueryInfo> getLiveQueryInfos() {
    List<QueryInfo> result = new LinkedList<>();
    synchronized (webuiLock) {
      result.addAll(liveQueryInfos.values());
    }
    return result;
  }

  /**
   * @param handle handle of SQLOperation.
   * @return display representing a particular SQLOperation.
   */
  public QueryInfo getQueryInfo(String handle) {
    synchronized (webuiLock) {
      if (historicalQueryInfos == null) {
        return null;
      }

      QueryInfo result = liveQueryInfos.get(handle);
      if (result != null) {
        return result;
      }
      return historicalQueryInfos.get(handle);
    }
  }

  public Operation getOperationByQueryId(String queryId) {
    return queryIdOperation.get(queryId);
  }

  public Operation getOperationByQueryTag(String queryTag) {
    String queryId = queryTagToIdMap.get(queryTag);
    if (queryId != null) {
      return getOperationByQueryId(queryId);
    }
    return null;
  }
}
