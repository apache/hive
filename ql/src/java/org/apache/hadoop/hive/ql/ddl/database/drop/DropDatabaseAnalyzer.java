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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

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
    String databaseName = unescapeIdentifier(root.getChild(0).getText());
    boolean ifExists = root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null;
    boolean cascade = root.getFirstChildWithType(HiveParser.TOK_CASCADE) != null;
    boolean isSoftDelete = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

    Database database = getDatabase(databaseName, !ifExists);
    if (database == null) {
      return;
    }
    // if cascade=true, then we need to authorize the drop table action as well, and add the tables to the outputs
    boolean isDbLevelLock = true;
    if (cascade) {
      try {
        List<Table> tables = db.getAllTableObjects(databaseName);
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
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }
    inputs.add(new ReadEntity(database));
    if (isDbLevelLock) {
      WriteEntity.WriteType lockType = isSoftDelete ?
        WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
      outputs.add(new WriteEntity(database, lockType));
    }
    DropDatabaseDesc desc = new DropDatabaseDesc(databaseName, ifExists, cascade, new ReplicationSpec());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  @Override
  public boolean hasAcidResources() {
    // check DB tags once supported (i.e. ICEBERG_ONLY, ACID_ONLY, EXTERNAL_ONLY)
    return true;
  }
}
