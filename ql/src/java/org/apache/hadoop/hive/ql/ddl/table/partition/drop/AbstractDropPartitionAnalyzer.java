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

package org.apache.hadoop.hive.ql.ddl.table.partition.drop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Analyzer for drop partition commands.
 */
abstract class AbstractDropPartitionAnalyzer extends AbstractAlterTableAnalyzer {

  AbstractDropPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {

    boolean ifExists = (command.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null)
        || HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);
    // If the drop has to fail on non-existent partitions, we cannot batch expressions.
    // That is because we actually have to check each separate expression for existence.
    // We could do a small optimization for the case where expr has all columns and all
    // operators are equality, if we assume those would always match one partition (which
    // may not be true with legacy, non-normalized column values). This is probably a
    // popular case but that's kinda hacky. Let's not do it for now.
    boolean canGroupExprs = ifExists;

    boolean mustPurge = (command.getFirstChildWithType(HiveParser.KW_PURGE) != null);
    ReplicationSpec replicationSpec = new ReplicationSpec(command);

    Table table = null;
    try {
      table = getTable(tableName);
    } catch (SemanticException se){
      if (replicationSpec.isInReplicationScope() &&
            (
                (se.getCause() instanceof InvalidTableException)
                || (se.getMessage().contains(ErrorMsg.INVALID_TABLE.getMsg()))
            )){
        // If we're inside a replication scope, then the table not existing is not an error.
        // We just return in that case, no drop needed.
        return;
        // TODO : the contains message check is fragile, we should refactor SemanticException to be
        // queryable for error code, and not simply have a message
        // NOTE : IF_EXISTS might also want to invoke this, but there's a good possibility
        // that IF_EXISTS is stricter about table existence, and applies only to the ptn.
        // Therefore, ignoring IF_EXISTS here.
      } else {
        throw se;
      }
    }
    validateAlterTableType(table, AlterTableType.DROPPARTITION, expectView());

    Map<Integer, List<ExprNodeGenericFuncDesc>> partitionSpecs = ParseUtils.getFullPartitionSpecs(command, table,
        conf, canGroupExprs);
    if (partitionSpecs.isEmpty()) { // nothing to do
      return;
    }

    ReadEntity re = new ReadEntity(table);
    re.noLockNeeded();
    inputs.add(re);

    boolean dropPartUseBase = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_DROP_PARTITION_USE_BASE)
        || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED)
      && AcidUtils.isTransactionalTable(table);

    addTableDropPartsOutputs(table, partitionSpecs.values(), !ifExists, dropPartUseBase);

    AlterTableDropPartitionDesc desc =
        new AlterTableDropPartitionDesc(tableName, partitionSpecs, mustPurge, replicationSpec, !dropPartUseBase, table);

    if (desc.mayNeedWriteId()) {
      setAcidDdlDesc(desc);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  protected abstract boolean expectView();

  /**
   * Add the table partitions to be modified in the output, so that it is available for the
   * pre-execution hook. If the partition does not exist, throw an error if
   * throwIfNonExistent is true, otherwise ignore it.
   */
  private void addTableDropPartsOutputs(Table table, Collection<List<ExprNodeGenericFuncDesc>> partitionSpecs,
      boolean throwIfNonExistent, boolean  dropPartUseBase) throws SemanticException {
    WriteType writeType =
        dropPartUseBase ? WriteType.DDL_EXCL_WRITE : WriteType.DDL_EXCLUSIVE;

    for (List<ExprNodeGenericFuncDesc> specs : partitionSpecs) {
      for (ExprNodeGenericFuncDesc partitionSpec : specs) {
        List<Partition> parts = new ArrayList<>();

        boolean hasUnknown = false;
        try {
          hasUnknown = db.getPartitionsByExpr(table, partitionSpec, conf, parts);
        } catch (Exception e) {
          throw new SemanticException("Error fetching partitions for " + partitionSpec.getExprString() +
             ", message: " + e.getMessage(), e);
        }
        if (hasUnknown) {
          throw new SemanticException("Unexpected unknown partitions for " + partitionSpec.getExprString());
        }

        // TODO: ifExists could be moved to metastore. In fact it already supports that. Check it
        //       for now since we get parts for output anyway, so we can get the error message
        //       earlier... If we get rid of output, we can get rid of this.
        if (parts.isEmpty()) {
          if (throwIfNonExistent) {
            throw new SemanticException(ErrorMsg.INVALID_PARTITION.getMsg(partitionSpec.getExprString()));
          }
        }
        for (Partition p : parts) {
          outputs.add(new WriteEntity(p, writeType));
        }
      }
    }
  }
}
