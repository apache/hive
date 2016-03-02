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

package org.apache.hadoop.hive.ql.parse;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexUpdater {
  private List<LoadTableDesc> loadTableWork;
  private HiveConf conf;
  // Assumes one instance of this + single-threaded compilation for each query.
  private Hive hive;
  private List<Task<? extends Serializable>> tasks;
  private Set<ReadEntity> inputs;


  public IndexUpdater(List<LoadTableDesc> loadTableWork, Set<ReadEntity> inputs, Configuration conf) {
    this.loadTableWork = loadTableWork;
    this.inputs = inputs;
    this.conf = new HiveConf(conf, IndexUpdater.class);
    this.tasks = new LinkedList<Task<? extends Serializable>>();
  }

  public IndexUpdater(LoadTableDesc loadTableWork, Set<ReadEntity> inputs,
      Configuration conf) {
    this.loadTableWork = new LinkedList<LoadTableDesc>();
    this.loadTableWork.add(loadTableWork);
    this.conf = new HiveConf(conf, IndexUpdater.class);
    this.tasks = new LinkedList<Task<? extends Serializable>>();
    this.inputs = inputs;
  }

  public List<Task<? extends Serializable>> generateUpdateTasks() throws
    HiveException {
    hive = Hive.get(this.conf);
    for (LoadTableDesc ltd : loadTableWork) {
      TableDesc td = ltd.getTable();
      Table srcTable = hive.getTable(td.getTableName());
      List<Index> tblIndexes = IndexUtils.getAllIndexes(srcTable, (short)-1);
      Map<String, String> partSpec = ltd.getPartitionSpec();
      if (partSpec == null || partSpec.size() == 0) {
        //unpartitioned table, update whole index
        doIndexUpdate(tblIndexes);
      } else {
        doIndexUpdate(tblIndexes, partSpec);
      }
    }
    return tasks;
  }

  private void doIndexUpdate(List<Index> tblIndexes) throws HiveException {
    Driver driver = new Driver(this.conf);
    for (Index idx : tblIndexes) {
      StringBuilder sb = new StringBuilder();
      sb.append("ALTER INDEX ");
      sb.append(idx.getIndexName());
      sb.append(" ON ");
      sb.append(idx.getDbName()).append('.');
      sb.append(idx.getOrigTableName());
      sb.append(" REBUILD");
      driver.compile(sb.toString(), false);
      tasks.addAll(driver.getPlan().getRootTasks());
      inputs.addAll(driver.getPlan().getInputs());
    }
  }

  private void doIndexUpdate(List<Index> tblIndexes, Map<String, String>
      partSpec) throws HiveException {
    for (Index index : tblIndexes) {
      if (containsPartition(index, partSpec)) {
        doIndexUpdate(index, partSpec);
      }
    }
  }

  private void doIndexUpdate(Index index, Map<String, String> partSpec) throws
    HiveException {
    StringBuilder ps = new StringBuilder();
    boolean first = true;
    ps.append("(");
    for (String key : partSpec.keySet()) {
      if (!first) {
        ps.append(", ");
      } else {
        first = false;
      }
      ps.append(key);
      ps.append("=");
      ps.append(partSpec.get(key));
    }
    ps.append(")");
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER INDEX ");
    sb.append(index.getIndexName());
    sb.append(" ON ");
    sb.append(index.getDbName()).append('.');
    sb.append(index.getOrigTableName());
    sb.append(" PARTITION ");
    sb.append(ps.toString());
    sb.append(" REBUILD");
    Driver driver = new Driver(this.conf);
    driver.compile(sb.toString(), false);
    tasks.addAll(driver.getPlan().getRootTasks());
    inputs.addAll(driver.getPlan().getInputs());
  }

  private boolean containsPartition(Index index, Map<String, String> partSpec)
      throws HiveException {
    String[] qualified = Utilities.getDbTableName(index.getDbName(), index.getIndexTableName());
    Table indexTable = hive.getTable(qualified[0], qualified[1]);
    List<Partition> parts = hive.getPartitions(indexTable, partSpec);
    return (parts == null || parts.size() == 0);
  }
}
