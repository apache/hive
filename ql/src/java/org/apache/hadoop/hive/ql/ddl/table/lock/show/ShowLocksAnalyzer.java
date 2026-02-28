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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

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

    String fullyQualifiedTableName = null;
    Map<String, String> partitionSpec = null;
    boolean isExtended = false;
    if (root.getChildCount() >= 1) {
      // table for which show locks is being executed
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        if (child.getType() == HiveParser.TOK_TABTYPE) {
          fullyQualifiedTableName = DDLUtils.getFQName((ASTNode) child.getChild(0));
          // get partition metadata if partition specified
          if (child.getChildCount() == 2) {
            ASTNode partitionSpecNode = (ASTNode) child.getChild(1);
            partitionSpec = getValidatedPartSpec(getTable(fullyQualifiedTableName), partitionSpecNode, conf, false);
          }
        } else if (child.getType() == HiveParser.KW_EXTENDED) {
          isExtended = true;
        }
      }
    }

    String catName = null;
    String dbName = null;
    String tableName = null;

    if (fullyQualifiedTableName != null) {
      String defaultDatabase = SessionState.get() != null
          ? SessionState.get().getCurrentDatabase()
          : Warehouse.DEFAULT_DATABASE_NAME;
      TableName fullyQualifiedTableNameObject = TableName.fromString(fullyQualifiedTableName,
          HiveUtils.getCurrentCatalogOrDefault(conf), defaultDatabase);
      catName = fullyQualifiedTableNameObject.getCat();
      dbName = fullyQualifiedTableNameObject.getDb();
      tableName = fullyQualifiedTableNameObject.getTable();

      if (getCatalog(catName) == null) {
        throw new SemanticException(ErrorMsg.CATALOG_NOT_EXISTS, catName);
      } else if (getDatabase(catName, dbName, true) == null) {
        throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS);
      }
    }

    assert txnManager != null : "Transaction manager should be set before calling analyze";
    ShowLocksDesc desc =
        new ShowLocksDesc(ctx.getResFile(), catName, dbName, tableName, partitionSpec,
            isExtended, txnManager.useNewShowLocksFormat());
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(desc.getTableName() != null ? desc.getTblSchema() : desc.getSchema()));

    // Need to initialize the lock manager
    ctx.setNeedLockMgr(true);
  }
}
