/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.spark;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Explain(displayName = "Spark Partition Pruning Sink Operator")
public class SparkPartitionPruningSinkDesc extends AbstractOperatorDesc {

  public static class DPPTargetInfo {
    // column in the target table that will be pruned against
    public String columnName;
    // type of target column
    public String columnType;
    // the partition column we're interested in
    public ExprNodeDesc partKey;
    public MapWork work;

    DPPTargetInfo(String columnName, String columnType, ExprNodeDesc partKey, MapWork work) {
      this.columnName = columnName;
      this.columnType = columnType;
      this.partKey = partKey;
      this.work = work;
    }
  }

  private List<DPPTargetInfo> targetInfos = new ArrayList<>();

  private TableDesc table;

  private transient TableScanOperator tableScan;

  private Path path;

  public List<DPPTargetInfo> getTargetInfos() {
    return targetInfos;
  }

  private void assertSingleTarget() {
    Preconditions.checkState(targetInfos.size() < 2, "The DPP sink has multiple targets.");
  }

  public String getTargetColumnName() {
    assertSingleTarget();
    return targetInfos.isEmpty() ? null : targetInfos.get(0).columnName;
  }

  public String getTargetColumnType() {
    assertSingleTarget();
    return targetInfos.isEmpty() ? null : targetInfos.get(0).columnType;
  }

  public ExprNodeDesc getTargetPartKey() {
    assertSingleTarget();
    return targetInfos.isEmpty() ? null : targetInfos.get(0).partKey;
  }

  public MapWork getTargetMapWork() {
    assertSingleTarget();
    return targetInfos.isEmpty() ? null : targetInfos.get(0).work;
  }

  public void addTarget(String colName, String colType, ExprNodeDesc partKey, MapWork mapWork) {
    targetInfos.add(new DPPTargetInfo(colName, colType, partKey, mapWork));
  }

  public void setTargetMapWork(MapWork mapWork) {
    Preconditions.checkState(targetInfos.size() == 1,
        "The DPP sink should have exactly one target.");
    targetInfos.get(0).work = mapWork;
    // in order to make the col name unique, prepend the targetId
    targetInfos.get(0).columnName = SparkUtilities.getWorkId(mapWork) + ":" +
        targetInfos.get(0).columnName;
  }

  Path getTmpPathOfTargetWork() {
    return targetInfos.isEmpty() ? null : targetInfos.get(0).work.getTmpPathForPartitionPruning();
  }

  @Explain(displayName = "tmp Path", explainLevels = {Explain.Level.EXTENDED})
  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  @Explain(displayName = "target works")
  public String getTargetWorks() {
    return Arrays.toString(targetInfos.stream().map(info -> info.work.getName()).toArray());
  }

  public TableScanOperator getTableScan() {
    return tableScan;
  }

  public void setTableScan(TableScanOperator tableScan) {
    this.tableScan = tableScan;
  }

  @Explain(displayName = "Target column")
  public String displayTargetColumns() {
    return Arrays.toString(targetInfos.stream().map(
        info -> info.columnName + " (" + info.columnType + ")").toArray());
  }

  public TableDesc getTable() {
    return table;
  }

  public void setTable(TableDesc table) {
    this.table = table;
  }

  @Explain(displayName = "partition key expr")
  public String getPartKeyStrings() {
    return Arrays.toString(targetInfos.stream().map(
        info -> info.partKey.getExprString()).toArray());
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      SparkPartitionPruningSinkDesc otherDesc = (SparkPartitionPruningSinkDesc) other;
      return getTable().equals(otherDesc.getTable());
    }
    return false;
  }
}
