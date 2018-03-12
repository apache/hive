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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    public transient TableScanOperator tableScan;

    DPPTargetInfo(String columnName, String columnType, ExprNodeDesc partKey, MapWork work,
        TableScanOperator tableScan) {
      this.columnName = columnName;
      this.columnType = columnType;
      this.partKey = partKey;
      this.work = work;
      this.tableScan = tableScan;
    }
  }

  private List<DPPTargetInfo> targetInfos = new ArrayList<>();

  private TableDesc table;

  private Path path;

  public List<DPPTargetInfo> getTargetInfos() {
    return targetInfos;
  }

  public void addTarget(String colName, String colType, ExprNodeDesc partKey, MapWork mapWork,
      TableScanOperator tableScan) {
    targetInfos.add(new DPPTargetInfo(colName, colType, partKey, mapWork, tableScan));
  }

  public Path getTmpPathOfTargetWork() {
    return targetInfos.isEmpty() ? null : targetInfos.get(0).work.getTmpPathForPartitionPruning();
  }

  @Explain(displayName = "tmp Path", explainLevels = {Explain.Level.EXTENDED})
  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public String getTargetWorks() {
    return Arrays.toString(targetInfos.stream().map(info -> info.work.getName()).toArray());
  }

  public String getTableScanNames() {
    return Arrays.toString(targetInfos.stream().map(info -> info.tableScan.getName()).toArray());
  }

  @Signature
  public TableDesc getTable() {
    return table;
  }

  public void setTable(TableDesc table) {
    this.table = table;
  }

  @Explain(displayName = "Target Columns")
  public String displayTargetColumns() {
    // The target column list has the format "TargetWork -> [colName:colType(expression), ...], ..."
    Map<String, List<String>> map = new TreeMap<>();
    for (DPPTargetInfo info : targetInfos) {
      List<String> columns = map.computeIfAbsent(info.work.getName(), v -> new ArrayList<>());
      String name = info.columnName.substring(info.columnName.indexOf(':') + 1);
      columns.add(name + ":" + info.columnType + " (" + info.partKey.getExprString() + ")");
    }
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    for (String work : map.keySet()) {
      if (builder.length() > 1) {
        builder.append(", ");
      }
      builder.append(work).append(" -> ").append(map.get(work));
    }
    builder.append("]");
    return builder.toString();
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      SparkPartitionPruningSinkDesc otherDesc = (SparkPartitionPruningSinkDesc) other;
      return getTable().equals(otherDesc.getTable());
    }
    return false;
  }

  public void removeTarget(String name) {
    List<DPPTargetInfo> toRemove = new ArrayList<>();
    for (DPPTargetInfo targetInfo : targetInfos) {
      if (targetInfo.work.getName().equals(name)) {
        toRemove.add(targetInfo);
      }
    }
    targetInfos.removeAll(toRemove);
  }
}
