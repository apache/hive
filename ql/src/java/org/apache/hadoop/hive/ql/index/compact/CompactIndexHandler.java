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

package org.apache.hadoop.hive.ql.index.compact;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.AbstractIndexHandler;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class CompactIndexHandler extends AbstractIndexHandler {
  
  private Configuration configuration;

  @Override
  public void analyzeIndexDefinition(Table baseTable, Index index,
      Table indexTable) throws HiveException {
    StorageDescriptor storageDesc = index.getSd();
    if (this.usesIndexTable() && indexTable != null) {
      StorageDescriptor indexTableSd = storageDesc.clone();
      List<FieldSchema> indexTblCols = indexTableSd.getCols();
      FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
      indexTblCols.add(bucketFileName);
      FieldSchema offSets = new FieldSchema("_offsets", "array<bigint>", "");
      indexTblCols.add(offSets);
      indexTable.setSd(indexTableSd);
    }
  }

  @Override
  public List<Task<?>> generateIndexBuildTaskList(
      org.apache.hadoop.hive.ql.metadata.Table baseTbl,
      org.apache.hadoop.hive.metastore.api.Index index,
      List<Partition> indexTblPartitions,
      List<Partition> baseTblPartitions,
      org.apache.hadoop.hive.ql.metadata.Table indexTbl,
      Hive db) throws HiveException {
    try {

      TableDesc desc = Utilities.getTableDesc(indexTbl);

      List<Partition> newBaseTblPartitions = new ArrayList<Partition>();

      List<Task<?>> indexBuilderTasks = new ArrayList<Task<?>>();

      if (!baseTbl.isPartitioned()) {
        // the table does not have any partition, then create index for the
        // whole table
        Task<?> indexBuilder = getIndexBuilderMapRedTask(index.getSd().getCols(), false,
            new PartitionDesc(desc, null), indexTbl.getTableName(),
            new PartitionDesc(Utilities.getTableDesc(baseTbl), null), 
            baseTbl.getTableName(), db, indexTbl.getDbName());
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
          if (basePart == null)
            throw new RuntimeException(
                "Partitions of base table and index table are inconsistent.");
          // for each partition, spawn a map reduce task.
          Task<?> indexBuilder = getIndexBuilderMapRedTask(index.getSd().getCols(), true,
              new PartitionDesc(indexPart), indexTbl.getTableName(),
              new PartitionDesc(basePart), baseTbl.getTableName(), db, indexTbl.getDbName());
          
          indexBuilderTasks.add(indexBuilder);
        }
      }
      return indexBuilderTasks;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private Task<?> getIndexBuilderMapRedTask(List<FieldSchema> indexField, boolean partitioned,
      PartitionDesc indexTblPartDesc, String indexTableName,
      PartitionDesc baseTablePartDesc, String baseTableName, Hive db, String dbName) {
    
    String indexCols = MetaStoreUtils.getColumnNamesFromFieldSchema(indexField);

    //form a new insert overwrite query.
    StringBuilder command= new StringBuilder();
    LinkedHashMap<String, String> partSpec = indexTblPartDesc.getPartSpec();

    command.append("INSERT OVERWRITE TABLE " + indexTableName );
    if (partitioned && indexTblPartDesc != null) {
      command.append(" PARTITION ( ");
      List<String> ret = getPartKVPairStringArray(partSpec);
      for (int i = 0; i < ret.size(); i++) {
        String partKV = ret.get(i);
        command.append(partKV);
        if (i < ret.size() - 1)
          command.append(",");
      }
      command.append(" ) ");
    }
    
    command.append(" SELECT ");
    command.append(indexCols);
    command.append(",");

    command.append(VirtualColumn.FILENAME.getName());
    command.append(",");
    command.append(" collect_set (");
    command.append(VirtualColumn.BLOCKOFFSET.getName());
    command.append(") ");
    command.append(" FROM " + baseTableName );
    LinkedHashMap<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
    if(basePartSpec != null) {
      command.append(" WHERE ");
      List<String> pkv = getPartKVPairStringArray(basePartSpec);
      for (int i = 0; i < pkv.size(); i++) {
        String partKV = pkv.get(i);
        command.append(partKV);
        if (i < pkv.size() - 1)
          command.append(" AND ");
      }
    }
    command.append(" GROUP BY ");
    command.append(indexCols + ", " + VirtualColumn.FILENAME.getName());

    Driver driver = new Driver(db.getConf());
    driver.compile(command.toString());

    Task<?> rootTask = driver.getPlan().getRootTasks().get(0);
    
    IndexMetadataChangeWork indexMetaChange = new IndexMetadataChangeWork(partSpec, indexTableName, dbName);
    IndexMetadataChangeTask indexMetaChangeTsk = new IndexMetadataChangeTask(); 
    indexMetaChangeTsk.setWork(indexMetaChange);
    rootTask.addDependentTask(indexMetaChangeTsk);

    return rootTask;
  }

  private List<String> getPartKVPairStringArray(
      LinkedHashMap<String, String> partSpec) {
    List<String> ret = new ArrayList<String>(partSpec.size());
    Iterator<Entry<String, String>> iter = partSpec.entrySet().iterator();
    while (iter.hasNext()) {
      StringBuilder sb = new StringBuilder();
      Entry<String, String> p = iter.next();
      sb.append(HiveUtils.unparseIdentifier(p.getKey()));
      sb.append(" = ");
      sb.append("'");
      sb.append(p.getValue());
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
