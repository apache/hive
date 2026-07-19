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

package org.apache.hadoop.hive.ql.optimizer.metainfo.query;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

import java.util.List;

/**
 * Resolves Join Operation metainformation (tablename, join column names, etc).
 *
 */
public class JoinOperationMetadataResolver {

  private String[] keyNames;
  private String[] tableAliases;

  private static final String UNKNOWN = "unknown";

  /**
   * Resolves the original table name (or query alias) and join key column names
   * for each join input position at <em>compile time</em>, while the full operator
   * tree is still available.
   *
   */
  public void resolveJoinMetadata(JoinOperator joinOp) {
    List<Operator<? extends OperatorDesc>> parents = joinOp.getParentOperators();
    if (parents == null || parents.isEmpty()) {
      return;
    }

    int numPositions = parents.size();
    keyNames = new String[numPositions];
    tableAliases = new String[numPositions];

    for (int pos = 0; pos < numPositions; pos++) {
      Operator<? extends OperatorDesc> parent = parents.get(pos);
      if (parent == null) {
        keyNames[pos] = UNKNOWN;
        tableAliases[pos] = UNKNOWN;
        continue;
      }

      resolveTableName(parent, pos);
      resolveColumns(parent, pos);
    }

  }

  private void resolveColumns(Operator<? extends OperatorDesc> parent, int pos) {
    if (!(parent instanceof ReduceSinkOperator)) {
      keyNames[pos] = UNKNOWN;
      return;
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) parent;
    ReduceSinkDesc rsConf = rsOp.getConf();
    if (rsConf == null) {
      keyNames[pos] = UNKNOWN;
      return;
    }

    List<ExprNodeDesc> keyCols = rsConf.getKeyCols();
    if (keyCols == null || keyCols.isEmpty()) {
      keyNames[pos] = UNKNOWN;
      return;
    }

    List<Operator<? extends OperatorDesc>> rsParents = rsOp.getParentOperators();
    RowSchema inputSchema =
        (rsParents != null && !rsParents.isEmpty() && rsParents.get(0) != null) ? rsParents.get(0).getSchema() : null;

    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < keyCols.size(); k++) {
      if (k > 0) {
        sb.append(", ");
      }
      ExprNodeDesc keyExpr = keyCols.get(k);
      sb.append(resolveKeyColumnName(keyExpr, inputSchema));
    }
    keyNames[pos] = sb.toString();
  }

  private void resolveTableName(Operator<? extends OperatorDesc> parent, int pos) {
    TableScanOperator tso = OperatorUtils.findSingleOperatorUpstream(parent, TableScanOperator.class);
    if (tso != null && tso.getConf() != null) {
      String alias = tso.getConf().getAlias();
      String tableName = tso.getConf().getTableName();
      if (alias != null && !alias.isEmpty()) {
        tableAliases[pos] = alias;
      } else if (tableName != null && !tableName.isEmpty()) {
        tableAliases[pos] = tableName;
      } else {
        tableAliases[pos] = UNKNOWN;
      }
    } else {
      tableAliases[pos] = UNKNOWN;
    }
  }

  /**
   * Returns the most human-readable name for a single join-key expression.
   * Never returns {@code null}: falls back to {@code "unknown"} if all else fails.
   */
  private String resolveKeyColumnName(ExprNodeDesc keyExpr, RowSchema inputSchema) {
    if (keyExpr == null) {
      return UNKNOWN;
    }
    if (keyExpr instanceof ExprNodeColumnDesc) {
      String internalColName = ((ExprNodeColumnDesc) keyExpr).getColumn();
      if (internalColName != null && inputSchema != null) {
        ColumnInfo ci = inputSchema.getColumnInfo(internalColName);
        if (ci != null && ci.getAlias() != null && !ci.getAlias().isEmpty()) {
          return ci.getAlias();
        }
      }
      // Fall back to the internal column name, at least it's something.
      return internalColName != null ? internalColName : UNKNOWN;
    }
    // For computed expressions (UDFs, casts, …) use the expression string.
    String exprStr = keyExpr.getExprString();
    return exprStr != null && !exprStr.isEmpty() ? exprStr : UNKNOWN;
  }

  public String[] getKeyNames() {
    return keyNames;
  }

  public String[] getTableAliases() {
    return tableAliases;
  }
}
