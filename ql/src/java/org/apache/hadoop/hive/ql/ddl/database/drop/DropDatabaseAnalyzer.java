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

package org.apache.hadoop.hive.ql.ddl.database.drop;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.List;

/**
 * Analyzer for database dropping commands.
 */
@DDLType(types = HiveParser.TOK_DROPDATABASE)
public class DropDatabaseAnalyzer extends BaseSemanticAnalyzer {
  public DropDatabaseAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    Pair<String, String> catDbNamePair = getCatDbNamePair((ASTNode) root.getChild(0));
    boolean ifExists = root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null;
    boolean cascade = root.getFirstChildWithType(HiveParser.TOK_CASCADE) != null;
    boolean isSoftDelete = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

    String catalogName = catDbNamePair.getLeft();
    if (catalogName != null && getCatalog(catalogName) == null) {
      throw new SemanticException(ErrorMsg.CATALOG_NOT_EXISTS, catalogName);
    }
    String databaseName = catDbNamePair.getRight();
    Database database = getDatabase(catDbNamePair.getLeft(), catDbNamePair.getRight(), ifExists);
    if (database == null) {
      return;
    }
    // if cascade=true, then we need to authorize the drop table action as well, and add the tables to the outputs
    boolean isDbLevelLock = true;
    if (cascade) {
      Hive newDb = null;
      boolean allowClose = db.allowClose();
      try {
        // Set the allowClose to false so that its underlying client won't be closed in case of
        // the change of the thread-local Hive
        db.setAllowClose(false);
        HiveConf hiveConf = new HiveConf(conf);
        hiveConf.set("hive.metastore.client.filter.enabled", "false");
        newDb = Hive.get(hiveConf);
        List<Table> tables = newDb.getAllTableObjects(catalogName, databaseName);
        isDbLevelLock = !isSoftDelete || tables.stream().allMatch(
          table -> AcidUtils.isTableSoftDeleteEnabled(table, conf));
        for (Table table : tables) {
          WriteEntity.WriteType lockType = WriteEntity.WriteType.DDL_NO_LOCK;
          // Optimization used to limit number of requested locks. Check if table lock is needed or we could get away with single DB level lock,
          if (!isDbLevelLock) {
            lockType = AcidUtils.isTableSoftDeleteEnabled(table, conf) ?
              WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
          }
          outputs.add(new WriteEntity(table, lockType));
        }
        // fetch all the functions in the database
        List<Function> functions = db.getFunctionsInDb(catalogName, databaseName, ".*");
        for (Function func: functions) {
          outputs.add(new WriteEntity(func, WriteEntity.WriteType.DDL_NO_LOCK));
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      } finally {
        if (newDb != null) {
          try {
            // restore the newDb instance so that hive conf is restored for any other thread local calls.
            newDb = Hive.get(conf);
          } catch (HiveException e) {
            throw new RuntimeException(e);
          }
        }
        db.setAllowClose(allowClose);
        // The newDb and its underlying client would be closed while restoring the thread-local Hive
        Hive.set(db);
      }
    }
    inputs.add(new ReadEntity(database));
    if (isDbLevelLock) {
      WriteEntity.WriteType lockType = isSoftDelete ?
        WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
      outputs.add(new WriteEntity(database, lockType));
    }
    DropDatabaseDesc desc = new DropDatabaseDesc(catalogName, databaseName, ifExists, cascade, new ReplicationSpec());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  @Override
  public boolean isRequiresOpenTransaction() {
    // check DB tags once supported (i.e. ICEBERG_ONLY, ACID_ONLY, EXTERNAL_ONLY)
    return true;
  }
}
