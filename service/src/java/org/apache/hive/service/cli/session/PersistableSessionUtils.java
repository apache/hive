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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.store.HiveSessionSnapshot;
import org.apache.hive.service.cli.session.store.SessionStateStore;
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
   * Captures the current session state into a snapshot DTO.
   */
  public static HiveSessionSnapshot captureSnapshot(SessionHandle sessionHandle,
      String username, String ipAddress, SessionState sessionState,
      HiveConf sessionConf, TProtocolVersion protocol,
      long creationTime, long lastAccessTime) {
    List<String> jars = new ArrayList<>();
    String addedJarsStr = Utilities.getResourceFiles(sessionConf, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJarsStr)) {
      Collections.addAll(jars, addedJarsStr.split(","));
    }

    Map<String, String> tempTableDefs = new HashMap<>();
    if (sessionState != null && sessionState.getTempTables() != null) {
      for (Map.Entry<String, Map<String, Table>> dbEntry :
          sessionState.getTempTables().entrySet()) {
        String dbName = dbEntry.getKey();
        for (Map.Entry<String, Table> tableEntry : dbEntry.getValue().entrySet()) {
          String tableName = tableEntry.getKey();
          Table table = tableEntry.getValue();
          String ddl = generateTempTableDDL(tableName, table);
          if (ddl != null) {
            tempTableDefs.put(TableName.getDbTable(dbName, tableName), ddl);
          }
        }
      }
    }

    return HiveSessionSnapshot.builder()
        .sessionHandleId(storeKey(sessionHandle))
        .username(username)
        .ipAddress(ipAddress)
        .currentDatabase(sessionState != null ? sessionState.getCurrentDatabase() : null)
        .overriddenConfigurations(sessionState != null
            ? new HashMap<>(sessionState.getOverriddenConfigurations()) : null)
        .addedJars(jars)
        .tempTableDefinitions(tempTableDefs)
        .protocolVersion(protocol.getValue())
        .creationTime(creationTime)
        .lastAccessTime(lastAccessTime)
        .build();
  }

  /**
   * Generates the CREATE TEMPORARY TABLE DDL for a temp table,
   * including LOCATION so data can be recovered on shared storage.
   */
  public static String generateTempTableDDL(String tableName, Table table) {
    try {
      StringBuilder sb = new StringBuilder("CREATE TEMPORARY TABLE ");
      sb.append(tableName).append(" (");
      List<FieldSchema> cols = table.getCols();
      for (int i = 0; i < cols.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(cols.get(i).getName()).append(" ").append(cols.get(i).getType());
      }
      sb.append(")");
      if (table.getSerializationLib() != null) {
        sb.append(" ROW FORMAT SERDE '").append(table.getSerializationLib()).append("'");
      }
      if (table.getStorageHandler() != null) {
        sb.append(" STORED BY '").append(table.getStorageHandler().getClass().getName()).append("'");
      } else if (table.getInputFormatClass() != null) {
        sb.append(" STORED AS INPUTFORMAT '").append(table.getInputFormatClass().getName()).append("'");
        if (table.getOutputFormatClass() != null) {
          sb.append(" OUTPUTFORMAT '").append(table.getOutputFormatClass().getName()).append("'");
        }
      }
      if (table.getDataLocation() != null) {
        sb.append(" LOCATION '").append(table.getDataLocation()).append("'");
      }
      return sb.toString();
    } catch (Exception e) {
      LOG.warn("Failed to generate DDL for temp table: {}", tableName, e);
      return null;
    }
  }

  /**
   * Hydrates a recovered session from a snapshot: restores database, configs,
   * JARs, and temp tables.
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
      if (snapshot.getTempTableDefinitions() != null) {
        restoreTempTables(session, sessionState, snapshot.getTempTableDefinitions());
      }
    } catch (Exception e) {
      LOG.error("Failed to hydrate session: {}", session.getSessionHandle(), e);
      throw new HiveSQLException("Failed to hydrate recovered session", e);
    }
  }

  private static void restoreTempTables(HiveSession session, SessionState sessionState,
      Map<String, String> tempTableDefs) {
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
      } catch (Exception e) {
        LOG.warn("Failed to restore temporary table {}", entry.getKey(), e);
      }
    }
    sessionState.setCurrentDatabase(currentDb);
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
