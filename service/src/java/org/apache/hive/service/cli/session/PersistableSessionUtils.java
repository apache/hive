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

package org.apache.hive.service.cli.session;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import java.util.LinkedHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TempTable;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.store.HiveSessionSnapshot;
import org.apache.hive.service.cli.session.store.SessionStateStore;
import org.apache.hive.service.cli.session.store.TempTablePartitionSnapshot;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for the Persistable Sessions feature.
 * Extracted from SessionManager and HiveSessionImpl to keep
 * the feature's logic isolated from existing core classes.
 */
public final class PersistableSessionUtils {

  public enum FetchStrategy {
    NEVER,
    ALWAYS,
    FETCH_WHEN_MISSING
  }

  private static final Logger LOG = LoggerFactory.getLogger(PersistableSessionUtils.class);

  /**
   * Returns a store key that incorporates both the public and secret UUIDs
   * of the session handle, preventing hijacking via the public ID alone.
   */
  public static String storeKey(SessionHandle sessionHandle) {
    return sessionHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessionHandle.getHandleIdentifier().getSecretId().toString();
  }

  private static final Pattern STATE_CHANGING_PATTERN = Pattern.compile(
      "(?i)^\\s*(USE\\b|SET\\b|ADD\\s+(JAR|FILE)\\b|DELETE\\s+(JAR|FILE)\\b" +
          "|(CREATE|DROP)\\s+TEMPORARY\\s+(TABLE|FUNCTION)\\b).*");

  private static final Pattern INSERT_TARGET_PATTERN = Pattern.compile(
      "(?i)^\\s*INSERT\\s+(?:INTO|OVERWRITE)\\s+(?:TABLE\\s+)?([^\\s(]+)");
  private static final Pattern LOAD_TARGET_PATTERN = Pattern.compile(
      "(?i)^\\s*LOAD\\s+DATA\\s+(?:LOCAL\\s+)?INPATH\\s+.+\\s+INTO\\s+TABLE\\s+([^\\s(]+)");

  private PersistableSessionUtils() {
  }

  /**
   * Determines whether a SQL statement changes session state that should
   * be persisted (database, configs, JARs, temp tables, temp functions).
   */
  public static boolean isStateChangingCommand(String statement) {
    if (statement == null) {
      return false;
    }
    return STATE_CHANGING_PATTERN.matcher(statement).matches();
  }

  /**
   * Returns true when a finished statement may have changed persisted session state,
   * including DML that adds data or partition metadata to temporary tables.
   */
  public static boolean shouldPersistSnapshot(String statement, SessionState sessionState) {
    if (statement == null) {
      return false;
    }
    if (isStateChangingCommand(statement)) {
      return true;
    }
    String targetTable = extractTempTableDmlTarget(statement);
    return targetTable != null && isSessionTempTable(sessionState, targetTable);
  }

  /**
   * Extracts the target table from INSERT or LOAD DATA statements, or returns null.
   */
  static String extractTempTableDmlTarget(String statement) {
    if (statement == null) {
      return null;
    }
    java.util.regex.Matcher insertMatcher = INSERT_TARGET_PATTERN.matcher(statement);
    if (insertMatcher.find()) {
      return normalizeTableReference(insertMatcher.group(1));
    }
    java.util.regex.Matcher loadMatcher = LOAD_TARGET_PATTERN.matcher(statement);
    if (loadMatcher.find()) {
      return normalizeTableReference(loadMatcher.group(1));
    }
    return null;
  }

  private static String normalizeTableReference(String tableRef) {
    return tableRef.replace("`", "");
  }

  /**
   * Returns true if the given table reference resolves to a session-local temp table.
   */
  static boolean isSessionTempTable(SessionState sessionState, String tableReference) {
    if (sessionState == null || tableReference == null) {
      return false;
    }
    try {
      TableName tableName = TableName.fromString(tableReference, null,
          sessionState.getCurrentDatabase());
      Map<String, Map<String, Table>> tempTables = sessionState.getTempTables();
      if (tempTables == null || tempTables.isEmpty()) {
        return false;
      }
      Map<String, Table> dbTables = tempTables.get(tableName.getDb().toLowerCase());
      return dbTables != null && dbTables.containsKey(tableName.getTable().toLowerCase());
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Captures the current session state into a snapshot DTO.
   */
  public static HiveSessionSnapshot captureSnapshot(SessionHandle sessionHandle,
      String username, String ipAddress, SessionState sessionState,
      HiveConf sessionConf, TProtocolVersion protocol,
      long creationTime, long lastAccessTime) {
    List<String> jars = new ArrayList<>();
    List<String> files = new ArrayList<>();
    if (sessionState != null) {
      Set<String> jarSet = sessionState.list_resource(SessionState.ResourceType.JAR, null);
      if (jarSet != null) {
        jars.addAll(jarSet);
      }
      Set<String> fileSet = sessionState.list_resource(SessionState.ResourceType.FILE, null);
      if (fileSet != null) {
        files.addAll(fileSet);
      }
    }

    Map<String, String> tempTableDefs = new HashMap<>();
    Map<String, List<TempTablePartitionSnapshot>> tempTablePartitionDefs = new HashMap<>();
    if (sessionState != null && sessionState.getTempTables() != null) {
      DDLPlanUtils ddlPlanUtils = new DDLPlanUtils();
      for (Map.Entry<String, Map<String, Table>> dbEntry :
          sessionState.getTempTables().entrySet()) {
        String dbName = dbEntry.getKey();
        for (Map.Entry<String, Table> tableEntry : dbEntry.getValue().entrySet()) {
          String tableName = tableEntry.getKey();
          Table table = tableEntry.getValue();
          String tableKey = TableName.getDbTable(dbName, tableName);
          String ddl = generateTempTableDDL(ddlPlanUtils, table);
          if (ddl != null) {
            tempTableDefs.put(tableKey, ddl);
          }
          List<TempTablePartitionSnapshot> partitionSnapshots = captureTempTablePartitions(
              sessionState, dbName, tableName, table);
          if (!partitionSnapshots.isEmpty()) {
            tempTablePartitionDefs.put(tableKey, partitionSnapshots);
          }
        }
      }
    }

    List<String> tempFuncDefs = captureTempFunctions(sessionState);

    return HiveSessionSnapshot.builder()
        .sessionHandleId(storeKey(sessionHandle))
        .username(username)
        .ipAddress(ipAddress)
        .currentDatabase(sessionState != null ? sessionState.getCurrentDatabase() : null)
        .overriddenConfigurations(sessionState != null
            ? new HashMap<>(sessionState.getOverriddenConfigurations()) : null)
        .addedJars(jars)
        .addedFiles(files)
        .tempTableDefinitions(tempTableDefs)
        .tempTablePartitionDefinitions(tempTablePartitionDefs)
        .tempFunctionDefinitions(tempFuncDefs)
        .protocolVersion(protocol.getValue())
        .creationTime(creationTime)
        .lastAccessTime(lastAccessTime)
        .build();
  }

  /**
   * Generates the CREATE TEMPORARY TABLE DDL for a temp table using DDLPlanUtils,
   * which handles partitions, table properties, complex types, bucket specs, etc.
   */
  static String generateTempTableDDL(DDLPlanUtils ddlPlanUtils, Table table) {
    try {
      return ddlPlanUtils.getCreateTableCommand(table, true);
    } catch (Exception e) {
      LOG.warn("Failed to generate DDL for temp table: {}", table.getTableName(), e);
      return null;
    }
  }

  /**
   * Captures session-local partition metadata for a partitioned temp table.
   */
  static List<TempTablePartitionSnapshot> captureTempTablePartitions(SessionState sessionState,
      String dbName, String tableName, Table table) {
    List<TempTablePartitionSnapshot> partitions = new ArrayList<>();
    if (sessionState == null || !table.isPartitioned()) {
      return partitions;
    }
    Map<String, TempTable> tempPartitions = sessionState.getTempPartitions();
    if (tempPartitions == null || tempPartitions.isEmpty()) {
      return partitions;
    }
    String qualifiedKey = Warehouse.getQualifiedName(dbName.toLowerCase(), tableName.toLowerCase());
    TempTable tempTable = tempPartitions.get(qualifiedKey);
    if (tempTable == null) {
      return partitions;
    }
    for (org.apache.hadoop.hive.metastore.api.Partition apiPartition : tempTable.listPartitions()) {
      String location = apiPartition.getSd() != null ? apiPartition.getSd().getLocation() : null;
      partitions.add(new TempTablePartitionSnapshot(
          new ArrayList<>(apiPartition.getValues()), location));
    }
    return partitions;
  }

  /**
   * Captures temporary function definitions as CREATE TEMPORARY FUNCTION DDL statements.
   * Uses the passed sessionState's registry directly rather than the thread-local,
   * since the snapshot may be captured on a thread different from the session's own.
   */
  static List<String> captureTempFunctions(SessionState sessionState) {
    List<String> funcDefs = new ArrayList<>();
    if (sessionState == null) {
      return funcDefs;
    }
    Registry registry = sessionState.getSessionRegistry();
    if (registry == null) {
      return funcDefs;
    }
    for (String funcName : registry.getCurrentFunctionNames()) {
      try {
        FunctionInfo info = registry.getFunctionInfo(funcName);
        if (info == null || info.getFunctionType() != FunctionInfo.FunctionType.TEMPORARY) {
          continue;
        }
        String className = info.getClassName();
        if (className == null) {
          Class<?> funcClass = info.getFunctionClass();
          if (funcClass != null) {
            className = funcClass.getName();
          }
        }
        if (className == null) {
          continue;
        }
        StringBuilder ddl = new StringBuilder("CREATE TEMPORARY FUNCTION ");
        ddl.append(funcName).append(" AS '").append(className).append("'");
        FunctionResource[] resources = info.getResources();
        if (resources != null && resources.length > 0) {
          ddl.append(" USING");
          for (int i = 0; i < resources.length; i++) {
            if (i > 0) {
              ddl.append(",");
            }
            ddl.append(" ").append(resources[i].getResourceType().name())
                .append(" '").append(resources[i].getResourceURI()).append("'");
          }
        }
        funcDefs.add(ddl.toString());
      } catch (Exception e) {
        LOG.warn("Failed to capture temp function: {}", funcName, e);
      }
    }
    return funcDefs;
  }

  /**
   * Hydrates a recovered session from a snapshot: restores database, configs,
   * JARs, files, temp functions, and temp tables.
   */
  public static void hydrateSession(HiveSession session, HiveSessionSnapshot snapshot)
      throws HiveSQLException {
    try {
      SessionState sessionState = session.getSessionState();
      if (snapshot.getCurrentDatabase() != null) {
        sessionState.setCurrentDatabase(snapshot.getCurrentDatabase());
      }
      if (snapshot.getOverriddenConfigurations() != null) {
        for (Map.Entry<String, String> entry : snapshot.getOverriddenConfigurations().entrySet()) {
          session.getHiveConf().set(entry.getKey(), entry.getValue());
          sessionState.getOverriddenConfigurations().put(entry.getKey(), entry.getValue());
        }
      }
      if (snapshot.getAddedJars() != null) {
        for (String jar : snapshot.getAddedJars()) {
          sessionState.add_resource(SessionState.ResourceType.JAR, jar);
        }
      }
      if (snapshot.getAddedFiles() != null) {
        for (String file : snapshot.getAddedFiles()) {
          sessionState.add_resource(SessionState.ResourceType.FILE, file);
        }
      }
      if (snapshot.getTempFunctionDefinitions() != null) {
        restoreTempFunctions(session, snapshot.getTempFunctionDefinitions());
      }
      if (snapshot.getTempTableDefinitions() != null) {
        restoreTempTables(session, sessionState, snapshot.getTempTableDefinitions(),
            snapshot.getTempTablePartitionDefinitions());
      }
    } catch (Exception e) {
      LOG.error("Failed to hydrate session: {}", session.getSessionHandle(), e);
      throw new HiveSQLException("Failed to hydrate recovered session", e);
    }
  }

  private static void restoreTempFunctions(HiveSession session, List<String> tempFuncDefs) {
    for (String ddl : tempFuncDefs) {
      try {
        OperationHandle opHandle = session.executeStatement(ddl, null);
        session.closeOperation(opHandle);
      } catch (Exception e) {
        LOG.warn("Failed to restore temporary function: {}", ddl, e);
      }
    }
  }

  private static void restoreTempTables(HiveSession session, SessionState sessionState,
      Map<String, String> tempTableDefs, Map<String, List<TempTablePartitionSnapshot>> tempTablePartitionDefs)
      throws HiveSQLException {
    String currentDb = sessionState.getCurrentDatabase();
    for (Map.Entry<String, String> entry : tempTableDefs.entrySet()) {
      try {
        TableName tn = TableName.fromString(entry.getKey(), null, currentDb);
        String db = tn.getDb();
        if (!db.equals(sessionState.getCurrentDatabase())) {
          sessionState.setCurrentDatabase(db);
        }
        OperationHandle opHandle = session.executeStatement(entry.getValue(), null);
        session.closeOperation(opHandle);
        List<TempTablePartitionSnapshot> partitions = tempTablePartitionDefs != null
            ? tempTablePartitionDefs.get(entry.getKey()) : null;
        if (partitions != null && !partitions.isEmpty()) {
          Table table = sessionState.getTempTables().get(db).get(tn.getTable());
          restoreTempTablePartitions(sessionState, table, partitions);
        }
      } catch (Exception e) {
        LOG.warn("Failed to restore temporary table {}", entry.getKey(), e);
        throw new HiveSQLException("Failed to restore temporary table " + entry.getKey(), e);
      }
    }
    sessionState.setCurrentDatabase(currentDb);
  }

  private static void restoreTempTablePartitions(SessionState sessionState, Table table,
      List<TempTablePartitionSnapshot> partitions)
      throws HiveException, MetaException, AlreadyExistsException {
    String qualifiedKey = Warehouse.getQualifiedName(
        table.getDbName().toLowerCase(), table.getTableName().toLowerCase());
    TempTable tempTable = sessionState.getTempPartitions().get(qualifiedKey);
    if (tempTable == null) {
      throw new HiveException("TempTable partition metadata missing for " + qualifiedKey);
    }
    List<org.apache.hadoop.hive.metastore.api.Partition> toAdd = new ArrayList<>(partitions.size());
    List<FieldSchema> partCols = table.getPartitionKeys();
    for (TempTablePartitionSnapshot snapshot : partitions) {
      Map<String, String> partSpec = new LinkedHashMap<>();
      List<String> values = snapshot.getValues();
      for (int i = 0; i < partCols.size(); i++) {
        partSpec.put(partCols.get(i).getName(), values.get(i));
      }
      Path location = snapshot.getLocation() != null ? new Path(snapshot.getLocation()) : null;
      Partition qlPart = new Partition(table, partSpec, location);
      toAdd.add(qlPart.getTPartition());
    }
    tempTable.addPartitions(toAdd, true);
  }

  /**
   * Unwraps a HiveSession proxy to get the underlying HiveSessionImpl.
   * Returns null if the session cannot be unwrapped.
   */
  public static HiveSessionImpl unwrapSession(HiveSession session) {
    if (session instanceof HiveSessionImpl impl) {
      return impl;
    }
    if (Proxy.isProxyClass(session.getClass())) {
      InvocationHandler handler = Proxy.getInvocationHandler(session);
      if (handler instanceof HiveSessionProxy proxy) {
        HiveSession base = proxy.getBaseSession();
        if (base instanceof HiveSessionImpl impl) {
          return impl;
        }
      }
    }
    return null;
  }

  /**
   * Saves the session snapshot to the state store.
   */
  public static void saveSnapshot(SessionStateStore store, HiveSession session) {
    if (store == null) {
      return;
    }
    try {
      HiveSessionImpl impl = unwrapSession(session);
      if (impl == null) {
        return;
      }
      HiveSessionSnapshot snapshot = impl.captureSnapshot();
      store.saveSnapshot(storeKey(session.getSessionHandle()), snapshot);
    } catch (Exception e) {
      LOG.warn("Failed to save session snapshot for: {}", session.getSessionHandle(), e);
    }
  }

  /**
   * Deletes the session snapshot from the state store.
   */
  public static void deleteSnapshot(SessionStateStore store, SessionHandle sessionHandle) {
    if (store == null) {
      return;
    }
    try {
      store.deleteSnapshot(storeKey(sessionHandle));
    } catch (Exception e) {
      LOG.warn("Failed to delete session snapshot for: {}", sessionHandle, e);
    }
  }
}
