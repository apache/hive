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
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.io.IOUtils;

import com.google.common.collect.Collections2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Utils {
  private static Logger LOG = LoggerFactory.getLogger(Utils.class);
  public static final String BOOTSTRAP_DUMP_STATE_KEY_PREFIX = "bootstrap.dump.state.";

  public enum ReplDumpState {
    IDLE, ACTIVE
  }

  public static void writeOutput(List<String> values, Path outputFile, HiveConf hiveConf)
      throws SemanticException {
    DataOutputStream outStream = null;
    try {
      FileSystem fs = outputFile.getFileSystem(hiveConf);
      outStream = fs.create(outputFile);
      outStream.writeBytes((values.get(0) == null ? Utilities.nullStringOutput : values.get(0)));
      for (int i = 1; i < values.size(); i++) {
        outStream.write(Utilities.tabCode);
        outStream.writeBytes((values.get(i) == null ? Utilities.nullStringOutput : values.get(i)));
      }
      outStream.write(Utilities.newLineCode);
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(outStream);
    }
  }

  public static Iterable<? extends String> matchesDb(Hive db, String dbPattern) throws HiveException {
    if (dbPattern == null) {
      return db.getAllDatabases();
    } else {
      return db.getDatabasesByPattern(dbPattern);
    }
  }

  public static Iterable<? extends String> matchesTbl(Hive db, String dbName, String tblPattern)
      throws HiveException {
    if (tblPattern == null) {
      return getAllTables(db, dbName);
    } else {
      return db.getTablesByPattern(dbName, tblPattern);
    }
  }

  public static Collection<String> getAllTables(Hive db, String dbName) throws HiveException {
    return Collections2.filter(db.getAllTables(dbName),
            tableName -> {
              assert tableName != null;
              return !tableName.toLowerCase().startsWith(
                      SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase());
            });
  }

  public static String setDbBootstrapDumpState(Hive hiveDb, String dbName) throws HiveException {
    Database database = hiveDb.getDatabase(dbName);
    if (database == null) {
      return null;
    }

    Map<String, String> newParams = new HashMap<>();
    String uniqueKey = BOOTSTRAP_DUMP_STATE_KEY_PREFIX + UUID.randomUUID().toString();
    newParams.put(uniqueKey, ReplDumpState.ACTIVE.name());
    Map<String, String> params = database.getParameters();

    // if both old params are not null, merge them
    if (params != null) {
      params.putAll(newParams);
      database.setParameters(params);
    } else {
      // if one of them is null, replace the old params with the new one
      database.setParameters(newParams);
    }

    hiveDb.alterDatabase(dbName, database);
    LOG.info("REPL DUMP:: Set property for Database: {}, Property: {}, Value: {}",
            dbName, uniqueKey, Utils.ReplDumpState.ACTIVE.name());
    return uniqueKey;
  }

  public static void resetDbBootstrapDumpState(Hive hiveDb, String dbName,
                                               String uniqueKey) throws HiveException {
    Database database = hiveDb.getDatabase(dbName);
    if (database != null) {
      Map<String, String> params = database.getParameters();
      if ((params != null) && params.containsKey(uniqueKey)) {
        params.remove(uniqueKey);
        database.setParameters(params);
        hiveDb.alterDatabase(dbName, database);
        LOG.info("REPL DUMP:: Reset property for Database: {}, Property: {}", dbName, uniqueKey);
      }
    }
  }

  public static boolean isBootstrapDumpInProgress(Hive hiveDb, String dbName) throws HiveException {
    Database database = hiveDb.getDatabase(dbName);
    if (database == null) {
      return false;
    }

    Map<String, String> params = database.getParameters();
    if (params == null) {
      return false;
    }

    for (String key : params.keySet()) {
      if (key.startsWith(BOOTSTRAP_DUMP_STATE_KEY_PREFIX)
              && params.get(key).equals(ReplDumpState.ACTIVE.name())) {
        return true;
      }
    }
    return false;
  }

  /**
   * validates if a table can be exported, similar to EximUtil.shouldExport with few replication
   * specific checks.
   */
  public static Boolean shouldReplicate(ReplicationSpec replicationSpec, Table tableHandle,
      HiveConf hiveConf) {
    if (replicationSpec == null) {
      replicationSpec = new ReplicationSpec();
    }

    if (replicationSpec.isNoop() || tableHandle == null) {
      return false;
    }

    if (tableHandle.isNonNative()) {
      return false;
    }

    if (replicationSpec.isInReplicationScope()) {
      boolean isAcidTable = AcidUtils.isTransactionalTable(tableHandle);
      if (isAcidTable) {
        return hiveConf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_INCLUDE_ACID_TABLES);
      }
      return !tableHandle.isTemporary();
    }
    return true;
  }

  public static boolean shouldReplicate(NotificationEvent tableForEvent,
      ReplicationSpec replicationSpec, Hive db, HiveConf hiveConf) {
    Table table;
    try {
      table = db.getTable(tableForEvent.getDbName(), tableForEvent.getTableName());
    } catch (HiveException e) {
      LOG.info(
          "error while getting table info for" + tableForEvent.getDbName() + "." + tableForEvent
              .getTableName(), e);
      return false;
    }
    return shouldReplicate(replicationSpec, table, hiveConf);
  }
}
