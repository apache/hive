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
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Deque;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public abstract class MultiInsertSqlGenerator {
  private static final String INDENT = "  ";

  protected final Table targetTable;
  protected final String targetTableFullName;
  protected final HiveConf conf;
  protected final String subQueryAlias;
  protected StringBuilder queryStr;
  private Deque<StringBuilder> stack = new ArrayDeque<>();
  
  private int nextCteExprPos = 0;
  
  protected MultiInsertSqlGenerator(
      Table targetTable, String targetTableFullName, HiveConf conf, String subQueryAlias) {
    this.targetTable = targetTable;
    this.targetTableFullName = targetTableFullName;
    this.conf = conf;
    this.subQueryAlias = subQueryAlias;
    this.queryStr = new StringBuilder();
  }

  public Table getTargetTable() {
    return targetTable;
  }

  public String getTargetTableFullName() {
    return targetTableFullName;
  }

  public abstract void appendAcidSelectColumns(Context.Operation operation);

  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation) {
    appendAcidSelectColumnsForDeletedRecords(operation, true);
  }
  
  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation, boolean skipPrefix) {
    throw new UnsupportedOperationException();
  }

  public abstract List<String> getDeleteValues(Context.Operation operation);
  public abstract List<String> getSortKeys();

  public String qualify(String columnName) {
    if (isBlank(subQueryAlias)) {
      return columnName;
    }
    return String.format("%s.%s", subQueryAlias, columnName);
  }

  public void appendInsertBranch(String hintStr, List<String> values) {
    queryStr.append("INSERT INTO ").append(targetTableFullName);
    appendPartitionCols(targetTable);
    queryStr.append("\n");

    queryStr.append(INDENT);
    queryStr.append("SELECT ");
    if (isNotBlank(hintStr)) {
      queryStr.append(hintStr);
    }

    queryStr.append(StringUtils.join(values, ","));
    queryStr.append("\n");
  }

  public void appendDeleteBranch(String hintStr) {
    List<String> deleteValues = getDeleteValues(Context.Operation.DELETE);
    appendInsertBranch(hintStr, deleteValues);
  }

  public void appendPartitionColsOfTarget() {
    appendPartitionCols(targetTable);
  }

  /**
   * Append list of partition columns to Insert statement. If user specified partition spec, then
   * use it to get/set the value for partition column else use dynamic partition mode with no value.
   * Static partition mode:
   * INSERT INTO T PARTITION(partCol1=val1,partCol2...) SELECT col1, ... partCol1,partCol2...
   * Dynamic partition mode:
   * INSERT INTO T PARTITION(partCol1,partCol2...) SELECT col1, ... partCol1,partCol2...
   */
  public void appendPartitionCols(Table table) {
    // If the table is partitioned we have to put the partition() clause in
    List<FieldSchema> partCols = table.getPartCols();
    if (partCols == null || partCols.isEmpty()) {
      return;
    }
    queryStr.append(" partition (");
    appendCols(partCols);
    queryStr.append(")");
  }

  public void appendSortBy(List<String> keys) {
    if (keys.isEmpty()) {
      return;
    }
    queryStr.append(INDENT).append("SORT BY ");
    queryStr.append(StringUtils.join(keys, ","));
    queryStr.append("\n");
  }

  public void appendSortKeys() {
    appendSortBy(getSortKeys());
  }

  public MultiInsertSqlGenerator append(String sqlTextFragment) {
    queryStr.append(sqlTextFragment);
    return this;
  }

  @Override
  public String toString() {
    return queryStr.toString();
  }
  
  public void removeLastChar() {
    queryStr.setLength(queryStr.length() - 1);
  }

  public void appendPartColsOfTargetTableWithComma(String alias) {
    if (targetTable.getPartCols() == null || targetTable.getPartCols().isEmpty()) {
      return;
    }
    queryStr.append(',');
    appendCols(targetTable.getPartCols(), alias, null);
  }

  public void appendAllColsOfTargetTable(String prefix) {
    appendCols(targetTable.getAllCols(), null, prefix);
  }
  
  public void appendAllColsOfTargetTable() {
    appendCols(targetTable.getAllCols());
  }

  public void appendCols(List<FieldSchema> columns) {
    appendCols(columns, null, null);
  }

  public void appendCols(List<FieldSchema> columns, String alias, String prefix) {
    if (columns == null) {
      return;
    }

    String quotedAlias = null;
    if (isNotBlank(alias)) {
      quotedAlias = HiveUtils.unparseIdentifier(alias, this.conf);
    }

    boolean first = true;
    for (FieldSchema fschema : columns) {
      if (first) {
        first = false;
      } else {
        queryStr.append(", ");
      }

      if (quotedAlias != null) {
        queryStr.append(quotedAlias).append('.');
      }
      queryStr.append(HiveUtils.unparseIdentifier(fschema.getName(), this.conf));
      
      if (isNotBlank(prefix)) {
        queryStr.append(" AS ");
        String prefixedIdentifier = HiveUtils.unparseIdentifier(prefix + fschema.getName(), this.conf);
        queryStr.append(prefixedIdentifier);
      }
    }
  }

  public MultiInsertSqlGenerator appendTargetTableName() {
    queryStr.append(targetTableFullName);
    return this;
  }

  public MultiInsertSqlGenerator append(char c) {
    queryStr.append(c);
    return this;
  }

  public MultiInsertSqlGenerator indent() {
    queryStr.append(INDENT);
    return this;
  }

  public MultiInsertSqlGenerator appendSubQueryAlias() {
    queryStr.append(subQueryAlias);
    return this;
  }

  public MultiInsertSqlGenerator newCteExpr(){
    stack.push(queryStr);
    queryStr = new StringBuilder(nextCteExprPos > 0 ? ",\n" : "WITH ");
    return this;
  }
  
  public MultiInsertSqlGenerator addCteExpr(){
    queryStr = stack.pop().insert(nextCteExprPos, queryStr);
    nextCteExprPos = queryStr.length();
    return this;
  }
}
