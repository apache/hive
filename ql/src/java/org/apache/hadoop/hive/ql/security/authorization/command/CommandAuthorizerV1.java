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

package org.apache.hadoop.hive.ql.security.authorization.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Command authorization, old type.
 */
final class CommandAuthorizerV1 {
  private CommandAuthorizerV1() {
    throw new UnsupportedOperationException("CommandAuthorizerV1 should not be instantiated");
  }

  static void doAuthorization(HiveOperation op, BaseSemanticAnalyzer sem, SessionState ss,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws HiveException {
    if (op == null) {
      throw new HiveException("Operation should not be null");
    }

    Hive db = sem.getDb();
    HiveAuthorizationProvider authorizer = ss.getAuthorizer();

    authorizeOperation(op, sem, inputs, outputs, db, authorizer);
    authorizeOutputs(op, outputs, db, authorizer);
    authorizeInputs(op, sem, inputs, authorizer);
  }

  private static void authorizeOperation(HiveOperation op, BaseSemanticAnalyzer sem, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, Hive db,
      HiveAuthorizationProvider authorizer) throws HiveException {
    if (op.equals(HiveOperation.CREATEDATABASE) || op.equals(HiveOperation.ALTERDATABASE_LOCATION)) {
      authorizer.authorizeDbLevelOperations(op.getInputRequiredPrivileges(), op.getOutputRequiredPrivileges(),
          inputs, outputs);
    } else if (op.equals(HiveOperation.CREATETABLE_AS_SELECT) || op.equals(HiveOperation.CREATETABLE)) {
      authorizer.authorize(db.getDatabase(SessionState.get().getCurrentDatabase()), null,
          HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
    } else  if (op.equals(HiveOperation.IMPORT)) {
      ImportSemanticAnalyzer isa = (ImportSemanticAnalyzer) sem;
      if (!isa.existsTable()) {
        authorizer.authorize(db.getDatabase(SessionState.get().getCurrentDatabase()), null,
            HiveOperation.CREATETABLE_AS_SELECT.getOutputRequiredPrivileges());
      }
    }
  }

  private static void authorizeOutputs(HiveOperation op, Set<WriteEntity> outputs, Hive db,
      HiveAuthorizationProvider authorizer) throws HiveException {
    if (CollectionUtils.isEmpty(outputs)) {
      return;
    }

    for (WriteEntity write : outputs) {
      if (write.isDummy() || write.isPathType()) {
        continue;
      }
      if (write.getType() == Entity.Type.DATABASE) {
        if (!op.equals(HiveOperation.IMPORT)){
          authorizer.authorize(write.getDatabase(), null, op.getOutputRequiredPrivileges());
        }
        // We skip DB check for import here because we already handle it above as a CTAS check.
        continue;
      }

      if (write.getType() == WriteEntity.Type.PARTITION) {
        Partition part = db.getPartition(write.getTable(), write.getPartition().getSpec(), false);
        if (part != null) {
          authorizer.authorize(write.getPartition(), null, op.getOutputRequiredPrivileges());
          continue;
        }
      }

      if (write.getTable() != null) {
        authorizer.authorize(write.getTable(), null, op.getOutputRequiredPrivileges());
      }
    }
  }

  private static void authorizeInputs(HiveOperation op, BaseSemanticAnalyzer sem, Set<ReadEntity> inputs,
      HiveAuthorizationProvider authorizer) throws HiveException {
    if (CollectionUtils.isEmpty(inputs)) {
      return;
    }

    Map<String, Boolean> tableUsePartLevelAuth = getTableUsePartLevelAuth(inputs);

    // column authorization is checked through table scan operators.
    Map<Table, List<String>> tab2Cols = new HashMap<Table, List<String>>();
    Map<Partition, List<String>> part2Cols = new HashMap<Partition, List<String>>();
    getTablePartitionUsedColumns(op, sem, tab2Cols, part2Cols, tableUsePartLevelAuth);

    // cache the results for table authorization
    Set<String> tableAuthChecked = new HashSet<String>();
    for (ReadEntity read : inputs) {
      // if read is not direct, we do not need to check its autho.
      if (read.isDummy() || read.isPathType() || !read.isDirect()) {
        continue;
      }
      if (read.getType() == Entity.Type.DATABASE) {
        authorizer.authorize(read.getDatabase(), op.getInputRequiredPrivileges(), null);
        continue;
      }
      Table tbl = read.getTable();
      if (tbl.isView() && sem instanceof SemanticAnalyzer) {
        tab2Cols.put(tbl, sem.getColumnAccessInfo().getTableToColumnAccessMap().get(tbl.getCompleteName()));
      }
      if (read.getPartition() != null) {
        Partition partition = read.getPartition();
        tbl = partition.getTable();
        // use partition level authorization
        if (Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName()))) {
          List<String> cols = part2Cols.get(partition);
          if (cols != null && cols.size() > 0) {
            authorizer.authorize(partition.getTable(), partition, cols, op.getInputRequiredPrivileges(), null);
          } else {
            authorizer.authorize(partition, op.getInputRequiredPrivileges(), null);
          }
          continue;
        }
      }

      authorizeTable(op, authorizer, tableUsePartLevelAuth, tab2Cols, tableAuthChecked, tbl);
    }
  }

  private static Map<String, Boolean> getTableUsePartLevelAuth(Set<ReadEntity> inputs) {
    // determine if partition level privileges should be checked for input tables
    Map<String, Boolean> tableUsePartLevelAuth = new HashMap<String, Boolean>();
    for (ReadEntity read : inputs) {
      if (read.isDummy() || read.isPathType() || read.getType() == Entity.Type.DATABASE) {
        continue;
      }
      Table tbl = read.getTable();
      if ((read.getPartition() != null) || (tbl != null && tbl.isPartitioned())) {
        String tblName = tbl.getTableName();
        if (tableUsePartLevelAuth.get(tblName) == null) {
          boolean usePartLevelPriv = (tbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE") != null &&
              ("TRUE".equalsIgnoreCase(tbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))));
          if (usePartLevelPriv) {
            tableUsePartLevelAuth.put(tblName, Boolean.TRUE);
          } else {
            tableUsePartLevelAuth.put(tblName, Boolean.FALSE);
          }
        }
      }
    }
    return tableUsePartLevelAuth;
  }

  private static void getTablePartitionUsedColumns(HiveOperation op, BaseSemanticAnalyzer sem,
      Map<Table, List<String>> tab2Cols, Map<Partition, List<String>> part2Cols,
      Map<String, Boolean> tableUsePartLevelAuth) throws HiveException {
    // for a select or create-as-select query, populate the partition to column (par2Cols) or
    // table to columns mapping (tab2Cols)
    if (op.equals(HiveOperation.CREATETABLE_AS_SELECT) || op.equals(HiveOperation.QUERY)) {
      ParseContext parseCtx = sem.getParseContext();

      for (Map.Entry<String, TableScanOperator> topOpMap : parseCtx.getTopOps().entrySet()) {
        TableScanOperator tableScanOp = topOpMap.getValue();
        if (!tableScanOp.isInsideView()) {
          Table tbl = tableScanOp.getConf().getTableMetadata();
          List<String> cols = new ArrayList<String>();
          for (int id : tableScanOp.getNeededColumnIDs()) {
            cols.add(tbl.getCols().get(id).getName());
          }
          // map may not contain all sources, since input list may have been optimized out
          // or non-existent tho such sources may still be referenced by the TableScanOperator
          // if it's null then the partition probably doesn't exist so let's use table permission
          if (tbl.isPartitioned() && Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName()))) {
            String aliasId = topOpMap.getKey();

            PrunedPartitionList partsList = PartitionPruner.prune(tableScanOp, parseCtx, aliasId);
            Set<Partition> parts = partsList.getPartitions();
            for (Partition part : parts) {
              List<String> existingCols = part2Cols.get(part);
              if (existingCols == null) {
                existingCols = new ArrayList<String>();
              }
              existingCols.addAll(cols);
              part2Cols.put(part, existingCols);
            }
          } else {
            List<String> existingCols = tab2Cols.get(tbl);
            if (existingCols == null) {
              existingCols = new ArrayList<String>();
            }
            existingCols.addAll(cols);
            tab2Cols.put(tbl, existingCols);
          }
        }
      }
    }
  }

  private static void authorizeTable(HiveOperation op, HiveAuthorizationProvider authorizer,
      Map<String, Boolean> tableUsePartLevelAuth, Map<Table, List<String>> tab2Cols, Set<String> tableAuthChecked,
      Table tbl) throws HiveException {
    // if we reach here, it means it needs to do a table authorization check, and the table authorization may
    // have already happened because of other partitions
    if (tbl != null && !tableAuthChecked.contains(tbl.getTableName()) &&
        !(Boolean.TRUE.equals(tableUsePartLevelAuth.get(tbl.getTableName())))) {
      List<String> cols = tab2Cols.get(tbl);
      if (cols != null && cols.size() > 0) {
        authorizer.authorize(tbl, null, cols, op.getInputRequiredPrivileges(), null);
      } else {
        authorizer.authorize(tbl, op.getInputRequiredPrivileges(), null);
      }
      tableAuthChecked.add(tbl.getTableName());
    }
  }
}
