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


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;

public class IndexUpdater {
  private List<LoadTableDesc> loadTableWork;
  private HiveConf conf;
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
      List<Index> tblIndexes = srcTable.getAllIndexes((short)-1);
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
      sb.append(idx.getOrigTableName());
      sb.append(" REBUILD");
      driver.compile(sb.toString());
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
    Table indexTable = hive.getTable(index.getIndexTableName());
    List<Partition> parts = hive.getPartitions(indexTable, partSpec);
    return (parts == null || parts.size() == 0);
  }
}
