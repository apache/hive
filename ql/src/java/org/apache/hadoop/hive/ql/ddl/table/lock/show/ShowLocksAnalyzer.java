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

package org.apache.hadoop.hive.ql.ddl.table.lock.show;

import java.util.Map;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for show locks commands.
 */
@DDLType(types = HiveParser.TOK_SHOWLOCKS)
public class ShowLocksAnalyzer extends BaseSemanticAnalyzer {
  public ShowLocksAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());

    String tableName = null;
    Map<String, String> partitionSpec = null;
    boolean isExtended = false;
    if (root.getChildCount() >= 1) {
      // table for which show locks is being executed
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        if (child.getType() == HiveParser.TOK_TABTYPE) {
          tableName = DDLUtils.getFQName((ASTNode) child.getChild(0));
          // get partition metadata if partition specified
          if (child.getChildCount() == 2) {
            ASTNode partitionSpecNode = (ASTNode) child.getChild(1);
            partitionSpec = getValidatedPartSpec(getTable(tableName), partitionSpecNode, conf, false);
          }
        } else if (child.getType() == HiveParser.KW_EXTENDED) {
          isExtended = true;
        }
      }
    }

    assert txnManager != null : "Transaction manager should be set before calling analyze";
    ShowLocksDesc desc =
        new ShowLocksDesc(ctx.getResFile(), tableName, partitionSpec, isExtended, txnManager.useNewShowLocksFormat());
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(desc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }
}
