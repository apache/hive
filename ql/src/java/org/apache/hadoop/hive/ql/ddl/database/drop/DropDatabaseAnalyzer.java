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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for database dropping commands.
 */
@DDLType(type=HiveParser.TOK_DROPDATABASE)
public class DropDatabaseAnalyzer extends BaseSemanticAnalyzer {
  public DropDatabaseAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String databaseName = unescapeIdentifier(root.getChild(0).getText());
    boolean ifExists = root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null;
    boolean cascade = root.getFirstChildWithType(HiveParser.TOK_CASCADE) != null;

    Database database = getDatabase(databaseName, !ifExists);
    if (database == null) {
      return;
    }

    // if cascade=true, then we need to authorize the drop table action as well, and add the tables to the outputs
    if (cascade) {
      try {
        List<String> tableNames = db.getAllTables(databaseName);
        if (tableNames != null) {
          for (String tableName : tableNames) {
            Table table = getTable(databaseName, tableName, true);
            // We want no lock here, as the database lock will cover the tables,
            // and putting a lock will actually cause us to deadlock on ourselves.
            outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
          }
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
    }

    inputs.add(new ReadEntity(database));
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_EXCLUSIVE));

    DropDatabaseDesc desc = new DropDatabaseDesc(databaseName, ifExists, cascade, new ReplicationSpec());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
