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

package org.apache.hadoop.hive.ql.ddl.database.lock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for database locking commands.
 */
@DDLType(types = HiveParser.TOK_LOCKDB)
public class LockDatabaseAnalyzer extends BaseSemanticAnalyzer {
  public LockDatabaseAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String databaseName = unescapeIdentifier(root.getChild(0).getText());
    String mode = unescapeIdentifier(root.getChild(1).getText().toUpperCase());

    inputs.add(new ReadEntity(getDatabase(databaseName)));
    // Lock database operation is to acquire the lock explicitly, the operation itself doesn't need to be locked.
    // Set the WriteEntity as WriteType: DDL_NO_LOCK here, otherwise it will conflict with Hive's transaction.
    outputs.add(new WriteEntity(getDatabase(databaseName), WriteType.DDL_NO_LOCK));

    LockDatabaseDesc desc =
        new LockDatabaseDesc(databaseName, mode, HiveConf.getVar(conf, ConfVars.HIVE_QUERY_ID), ctx.getCmd());
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    ctx.setNeedLockMgr(true);
  }
}
