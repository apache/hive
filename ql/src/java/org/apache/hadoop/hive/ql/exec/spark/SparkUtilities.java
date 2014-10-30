/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.BytesWritable;

/**
 * Contains utilities methods used as part of Spark tasks
 */
public class SparkUtilities {

  // Used to save and retrieve IOContext for multi-insertion.
  public static final String MAP_IO_CONTEXT = "MAP_IO_CONTEXT";

  public static HiveKey copyHiveKey(HiveKey key) {
    HiveKey copy = new HiveKey();
    copy.setDistKeyLength(key.getDistKeyLength());
    copy.setHashCode(key.hashCode());
    copy.set(key);
    return copy;
  }

  public static BytesWritable copyBytesWritable(BytesWritable bw) {
    BytesWritable copy = new BytesWritable();
    copy.set(bw);
    return copy;
  }

  public static List<String> getRequiredCounterPrefix(SparkTask sparkTask, Hive db)
    throws HiveException, MetaException {

    List<String> prefixs = new LinkedList<String>();
    List<BaseWork> works = sparkTask.getWork().getAllWork();
    for (BaseWork baseWork : works) {
      Set<Operator<?>> operators = baseWork.getAllOperators();
      for (Operator<?> operator : operators) {
        if (operator instanceof TableScanOperator) {
          TableScanOperator tableScanOperator = (TableScanOperator) operator;
          TableScanDesc tableScanDesc = tableScanOperator.getConf();

          if (tableScanDesc.isGatherStats()) {
            List<Task<? extends Serializable>> childTasks = getChildTasks(sparkTask);
            for (Task<? extends Serializable> task : childTasks) {
              if (task instanceof StatsTask) {
                StatsTask statsTask = (StatsTask) task;
                StatsWork statsWork = statsTask.getWork();
                // ANALYZE command
                BaseSemanticAnalyzer.tableSpec tblSpec = statsWork.getTableSpecs();
                Table table = tblSpec.tableHandle;
                if (!table.isPartitioned()) {
                  prefixs.add(tableScanDesc.getStatsAggPrefix()); // non-partitioned
                } else {
                  for (Partition partition : tblSpec.partitions) {
                    String aggrPrefix = getAggregationPrefix(
                      table, partition.getSpec(), tableScanDesc.getMaxStatsKeyPrefixLength());
                    prefixs.add(aggrPrefix);
                  }
                }
              }
            }
          }
        } else if (operator instanceof FileSinkOperator) {
          FileSinkOperator fileSinkOperator = (FileSinkOperator) operator;
          FileSinkDesc fileSinkDesc = fileSinkOperator.getConf();

          if (fileSinkDesc.isGatherStats()) {
            List<Task<? extends Serializable>> childTasks = getChildTasks(sparkTask);
            for (Task<? extends Serializable> task : childTasks) {
              if (task instanceof MoveTask) {
                MoveTask moveTask = (MoveTask) task;
                MoveWork moveWork = moveTask.getWork();

                // INSERT OVERWRITE command
                LoadTableDesc tbd = moveWork.getLoadTableWork();
                Table table = db.getTable(tbd.getTable().getTableName());
                if (!table.isPartitioned()) {
                  prefixs.add(
                    getAggregationPrefix(table, null, fileSinkDesc.getMaxStatsKeyPrefixLength()));
                } else {
                  DynamicPartitionCtx dpCtx = tbd.getDPCtx();
                  if (dpCtx == null || dpCtx.getNumDPCols() == 0) {
                    // static partition
                    Map<String, String> partitionSpec = tbd.getPartitionSpec();
                    if (partitionSpec != null && !partitionSpec.isEmpty()) {
                      String aggrPrefix = getAggregationPrefix(
                        table, partitionSpec, fileSinkDesc.getMaxStatsKeyPrefixLength());
                      prefixs.add(aggrPrefix);
                    }
                  } else {
                    // dynamic partition
                  }
                }
              }
            }
          }
        }
      }
    }
    return prefixs;
  }

  private static String getAggregationPrefix(Table table, Map<String, String> partitionSpec, int maxKeyLength)
    throws MetaException {
    StringBuilder prefix = new StringBuilder();
    // prefix is of the form dbName.tblName
    prefix.append(table.getDbName()).append('.').append(table.getTableName());
    if (partitionSpec != null) {
      return Utilities.join(prefix.toString(), Warehouse.makePartPath(partitionSpec));
    }
    return Utilities.getHashedStatsPrefix(prefix.toString(), maxKeyLength);
  }

  private static List<Task<? extends Serializable>> getChildTasks(
    Task<? extends Serializable> rootTask) {

    List<Task<? extends Serializable>> tasks = new ArrayList<Task<? extends Serializable>>();
    fillChildTasks(tasks, rootTask);
    return tasks;
  }

  private static void fillChildTasks(
    List<Task<? extends Serializable>> tasks,
    Task<? extends Serializable> rootTask) {

    List<Task<? extends Serializable>> childTasks = rootTask.getChildTasks();
    tasks.add(rootTask);
    if (childTasks != null) {
      for (Task<? extends Serializable> task : childTasks) {
        fillChildTasks(tasks, task);
      }
    }
  }
}
