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

package org.apache.hadoop.hive.ql.ddl.table.lock;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for unlock table commands.
 */
@DDLType(types = HiveParser.TOK_UNLOCKTABLE)
public class UnlockTableAnalyzer extends BaseSemanticAnalyzer {
  public UnlockTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String tableName = getUnescapedName((ASTNode) root.getChild(0));
    List<Map<String, String>> partitionSpecs = getPartitionSpecs(getTable(tableName), root);

    // We only can have a single partition spec
    assert (partitionSpecs.size() <= 1);
    Map<String, String> partitionSpec = null;
    if (partitionSpecs.size() > 0) {
      partitionSpec = partitionSpecs.get(0);
    }

    UnlockTableDesc desc = new UnlockTableDesc(tableName, partitionSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }
}
