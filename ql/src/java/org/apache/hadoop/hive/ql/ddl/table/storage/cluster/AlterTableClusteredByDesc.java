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

package org.apache.hadoop.hive.ql.ddl.table.storage.cluster;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.util.DirectionUtils;

/**
 * DDL task description for ALTER TABLE ... CLUSTERED BY ... SORTED BY ... [INTO ... BUCKETS] commands.
 */
@Explain(displayName = "Clustered By", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableClusteredByDesc extends AbstractAlterTableDesc {
  private static final long serialVersionUID = 1L;

  private final int numberBuckets;
  private final List<String> bucketColumns;
  private final List<Order> sortColumns;

  public AlterTableClusteredByDesc(TableName tableName, Map<String, String> partitionSpec, int numberBuckets,
      List<String> bucketColumns, List<Order> sortColumns) throws SemanticException {
    super(AlterTableType.CLUSTERED_BY, tableName, partitionSpec, null, false, false, null);
    this.numberBuckets = numberBuckets;
    this.bucketColumns = bucketColumns;
    this.sortColumns = sortColumns;
  }

  @Explain(displayName = "number of buckets", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public int getNumberBuckets() {
    return numberBuckets;
  }

  @Explain(displayName = "bucket columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getBucketColumns() {
    return bucketColumns;
  }

  public List<Order> getSortColumns() {
    return sortColumns;
  }

  // Only for explaining
  @Explain(displayName = "sort columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public List<String> getSortColumnsExplain() {
    return sortColumns.stream()
        .map(t -> t.getCol() + " " + DirectionUtils.codeToText(t.getOrder()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean mayNeedWriteId() {
    return true;
  }
}
