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

package org.apache.hadoop.hive.ql.ddl.table.drop;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for table dropping commands.
 */
@DDLType(type=HiveParser.TOK_DROPTABLE)
public class DropTableAnalyzer extends BaseSemanticAnalyzer {
  public DropTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String tableName = getUnescapedName((ASTNode) root.getChild(0));
    boolean ifExists = (root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    boolean throwException = !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);

    Table table = getTable(tableName, throwException);
    if (table != null) {
      inputs.add(new ReadEntity(table));
      outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_EXCLUSIVE));
    }

    boolean purge = (root.getFirstChildWithType(HiveParser.KW_PURGE) != null);
    ReplicationSpec replicationSpec = new ReplicationSpec(root);
    DropTableDesc desc = new DropTableDesc(tableName, ifExists, purge, replicationSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
