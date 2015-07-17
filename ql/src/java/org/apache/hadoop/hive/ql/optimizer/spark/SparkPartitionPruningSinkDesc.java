/**
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

package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

@Explain(displayName = "Spark Partition Pruning Sink Operator")
public class SparkPartitionPruningSinkDesc extends AbstractOperatorDesc {

  // column in the target table that will be pruned against
  private String targetColumnName;

  private TableDesc table;

  private transient TableScanOperator tableScan;

  // the partition column we're interested in
  private ExprNodeDesc partKey;

  private Path path;

  private String targetWork;

  @Explain(displayName = "tmp Path", explainLevels = { Explain.Level.EXTENDED })
  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  @Explain(displayName = "target work")
  public String getTargetWork() {
    return this.targetWork;
  }

  public void setTargetWork(String targetWork) {
    this.targetWork = targetWork;
  }

  public TableScanOperator getTableScan() {
    return tableScan;
  }

  public void setTableScan(TableScanOperator tableScan) {
    this.tableScan = tableScan;
  }

  @Explain(displayName = "target column name")
  public String getTargetColumnName() {
    return targetColumnName;
  }

  public void setTargetColumnName(String targetColumnName) {
    this.targetColumnName = targetColumnName;
  }

  public ExprNodeDesc getPartKey() {
    return partKey;
  }

  public void setPartKey(ExprNodeDesc partKey) {
    this.partKey = partKey;
  }

  public TableDesc getTable() {
    return table;
  }

  public void setTable(TableDesc table) {
    this.table = table;
  }

  @Explain(displayName = "partition key expr")
  public String getPartKeyString() {
    return partKey.getExprString();
  }
}
