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

package org.apache.hadoop.hive.ql.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Index handler for indexes that use tables to store indexes.
 */
public abstract class TableBasedIndexHandler extends AbstractIndexHandler {
  protected Configuration configuration;

  @Override
  public List<Task<?>> generateIndexBuildTaskList(
      org.apache.hadoop.hive.ql.metadata.Table baseTbl,
      org.apache.hadoop.hive.metastore.api.Index index,
      List<Partition> indexTblPartitions, List<Partition> baseTblPartitions,
      org.apache.hadoop.hive.ql.metadata.Table indexTbl,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws HiveException {
    try {

      TableDesc desc = Utilities.getTableDesc(indexTbl);

      List<Partition> newBaseTblPartitions = new ArrayList<Partition>();

      List<Task<?>> indexBuilderTasks = new ArrayList<Task<?>>();

      if (!baseTbl.isPartitioned()) {
        // the table does not have any partition, then create index for the
        // whole table
        Task<?> indexBuilder = getIndexBuilderMapRedTask(inputs, outputs, index, false,
            new PartitionDesc(desc, null), indexTbl.getTableName(),
            new PartitionDesc(Utilities.getTableDesc(baseTbl), null),
            baseTbl.getTableName(), indexTbl.getDbName());
        indexBuilderTasks.add(indexBuilder);
      } else {

        // check whether the index table partitions are still exists in base
        // table
        for (int i = 0; i < indexTblPartitions.size(); i++) {
          Partition indexPart = indexTblPartitions.get(i);
          Partition basePart = null;
          for (int j = 0; j < baseTblPartitions.size(); j++) {
            if (baseTblPartitions.get(j).getName().equals(indexPart.getName())) {
              basePart = baseTblPartitions.get(j);
              newBaseTblPartitions.add(baseTblPartitions.get(j));
              break;
            }
          }
          if (basePart == null) {
            throw new RuntimeException(
                "Partitions of base table and index table are inconsistent.");
          }
          // for each partition, spawn a map reduce task.
          Task<?> indexBuilder = getIndexBuilderMapRedTask(inputs, outputs, index, true,
              new PartitionDesc(indexPart), indexTbl.getTableName(),
              new PartitionDesc(basePart), baseTbl.getTableName(), indexTbl.getDbName());
          indexBuilderTasks.add(indexBuilder);
        }
      }
      return indexBuilderTasks;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      Index index, boolean partitioned,
      PartitionDesc indexTblPartDesc, String indexTableName,
      PartitionDesc baseTablePartDesc, String baseTableName, String dbName) throws HiveException {
    return getIndexBuilderMapRedTask(inputs, outputs, index.getSd().getCols(),
        partitioned, indexTblPartDesc, indexTableName, baseTablePartDesc, baseTableName, dbName);
  }

  protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      List<FieldSchema> indexField, boolean partitioned,
      PartitionDesc indexTblPartDesc, String indexTableName,
      PartitionDesc baseTablePartDesc, String baseTableName, String dbName) throws HiveException {
    return null;
  }

  protected List<String> getPartKVPairStringArray(
      LinkedHashMap<String, String> partSpec) {
    List<String> ret = new ArrayList<String>(partSpec.size());
    Iterator<Entry<String, String>> iter = partSpec.entrySet().iterator();
    while (iter.hasNext()) {
      StringBuilder sb = new StringBuilder();
      Entry<String, String> p = iter.next();
      sb.append(HiveUtils.unparseIdentifier(p.getKey()));
      sb.append(" = ");
      sb.append("'");
      sb.append(HiveUtils.escapeString(p.getValue()));
      sb.append("'");
      ret.add(sb.toString());
    }
    return ret;
  }

  @Override
  public boolean usesIndexTable() {
    return true;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

}
