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

package org.apache.hadoop.hive.ql.parse;


import java.util.Set;

import javax.annotation.Nullable;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.dump.TableExport;
import org.apache.hadoop.hive.ql.plan.ExportWork;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;

/**
 * ExportSemanticAnalyzer.
 *
 */
public class ExportSemanticAnalyzer extends BaseSemanticAnalyzer {
  private boolean isMmExport = false;

  ExportSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    Task<ExportWork> task = analyzeExport(ast, null, db, conf, inputs, outputs);
    isMmExport = task.getWork().getMmContext() != null;
    rootTasks.add(task);
  }
  /**
   * @param acidTableName - table name in db.table format; not NULL if exporting Acid table
   */
  static Task<ExportWork> analyzeExport(ASTNode ast, @Nullable String acidTableName, Hive db,
      HiveConf conf, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
    Tree tableTree = ast.getChild(0);
    Tree toTree = ast.getChild(1);

    ReplicationSpec replicationSpec;
    if (ast.getChildCount() > 2) {
      // Replication case: export table <tbl> to <location> for replication
      replicationSpec = new ReplicationSpec((ASTNode) ast.getChild(2));
    } else {
      // Export case
      replicationSpec = new ReplicationSpec();
    }
    if (replicationSpec.getCurrentReplicationState() == null) {
      try {
        long currentEventId = db.getMSC().getCurrentNotificationEventId().getEventId();
        replicationSpec.setCurrentReplicationState(String.valueOf(currentEventId));
      } catch (Exception e) {
        throw new SemanticException("Error when getting current notification event ID", e);
      }
    }

    // initialize source table/partition
    TableSpec ts;

    try {
      ts = new TableSpec(db, conf, (ASTNode) tableTree, false, true);
    } catch (SemanticException sme) {
      if (!replicationSpec.isInReplicationScope()) throw sme;
      if ((sme.getCause() instanceof InvalidTableException)
            || (sme instanceof Table.ValidationFailureSemanticException)) {
        // If we're in replication scope, it's possible that we're running the export long after
        // the table was dropped, so the table not existing currently or being a different kind of
        // table is not an error - it simply means we should no-op, and let a future export
        // capture the appropriate state
        ts = null;
      } else {
        throw sme;
      }
    }

    if (ts != null && (ts.tableHandle.isView() || ts.tableHandle.isMaterializedView())) {
      throw new SemanticException("Views and Materialized Views can not be exported.");
    }

    // initialize export path
    String tmpPath = stripQuotes(toTree.getText());
    // All parsing is done, we're now good to start the export process
    TableExport.Paths exportPaths =
        new TableExport.Paths(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_PATH.getMsg(), ast), tmpPath, conf, false);
    // Note: this tableExport is actually never used other than for auth, and another one is
    //       created when the task is executed. So, we don't care about the correct MM state here.
    TableExport.AuthEntities authEntities = new TableExport(
        exportPaths, ts, replicationSpec, db, null, conf, null).getAuthEntities();
    inputs.addAll(authEntities.inputs);
    outputs.addAll(authEntities.outputs);
    String exportRootDirName = tmpPath;
    MmContext mmCtx = MmContext.createIfNeeded(ts == null ? null : ts.tableHandle);

    Utilities.FILE_OP_LOGGER.debug("Exporting table {}: MM context {}",
        ts == null ? null : ts.getTableName(), mmCtx);
      // Configure export work
    ExportWork exportWork = new ExportWork(exportRootDirName, ts, replicationSpec,
        ASTErrorUtils.getMsg(ErrorMsg.INVALID_PATH.getMsg(), ast), acidTableName, mmCtx);
    // Create an export task and add it as a root task
    return TaskFactory.get(exportWork);
  }
  
  @Override
  public boolean hasAcidReadWrite() {
    return isMmExport; // Full ACID export goes through UpdateDelete analyzer.
  }
}
