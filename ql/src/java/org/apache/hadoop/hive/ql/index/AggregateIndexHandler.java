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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;


/**
 * Index handler for indexes that have aggregate functions on indexed columns.
 *
 */
public class AggregateIndexHandler extends CompactIndexHandler {

    @Override
    public void analyzeIndexDefinition(Table baseTable, Index index,
        Table indexTable) throws HiveException {
      StorageDescriptor storageDesc = index.getSd();
      if (this.usesIndexTable() && indexTable != null) {
        StorageDescriptor indexTableSd = storageDesc.deepCopy();
        List<FieldSchema> indexTblCols = indexTableSd.getCols();
        FieldSchema bucketFileName = new FieldSchema("_bucketname", "string", "");
        indexTblCols.add(bucketFileName);
        FieldSchema offSets = new FieldSchema("_offsets", "array<bigint>", "");
        indexTblCols.add(offSets);
        Map<String, String> paraList = index.getParameters();

        if(paraList != null && paraList.containsKey("AGGREGATES")){
          String propValue = paraList.get("AGGREGATES");
          if(propValue.contains(",")){
            String[] aggFuncs = propValue.split(",");
            for (int i = 0; i < aggFuncs.length; i++) {
              createAggregationFunction(indexTblCols, aggFuncs[i]);
            }
          }else{
            createAggregationFunction(indexTblCols, propValue);
         }
        }
        indexTable.setSd(indexTableSd);
      }
    }

    private void createAggregationFunction(List<FieldSchema> indexTblCols, String property){
      String[] aggFuncCol = property.split("\\(");
      String funcName = aggFuncCol[0];
      String colName = aggFuncCol[1].substring(0, aggFuncCol[1].length() - 1);
      if(colName.contains("*")){
        colName = colName.replace("*", "all");
      }
      FieldSchema aggregationFunction =
        new FieldSchema("_" + funcName + "_of_" + colName + "", "bigint", "");
      indexTblCols.add(aggregationFunction);
    }

    @Override
    protected Task<?> getIndexBuilderMapRedTask(Set<ReadEntity> inputs,
        Set<WriteEntity> outputs,
        Index index, boolean partitioned,
        PartitionDesc indexTblPartDesc, String indexTableName,
        PartitionDesc baseTablePartDesc, String baseTableName, String dbName) {

      List<FieldSchema> indexField = index.getSd().getCols();
      String indexCols = HiveUtils.getUnparsedColumnNamesFromFieldSchema(indexField);

      //form a new insert overwrite query.
      StringBuilder command= new StringBuilder();
      Map<String, String> partSpec = indexTblPartDesc.getPartSpec();

      command.append("INSERT OVERWRITE TABLE " + HiveUtils.unparseIdentifier(indexTableName));
      if (partitioned && indexTblPartDesc != null) {
        command.append(" PARTITION ( ");
        List<String> ret = getPartKVPairStringArray((LinkedHashMap<String, String>) partSpec);
        for (int i = 0; i < ret.size(); i++) {
          String partKV = ret.get(i);
          command.append(partKV);
          if (i < ret.size() - 1) {
            command.append(",");
          }
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
      command.append(",");

      assert indexField.size()==1;

      Map<String, String> paraList = index.getParameters();
      if(paraList != null && paraList.containsKey("AGGREGATES")){
          command.append(paraList.get("AGGREGATES") + " ");
      }

      command.append(" FROM " + HiveUtils.unparseIdentifier(baseTableName));
      Map<String, String> basePartSpec = baseTablePartDesc.getPartSpec();
      if(basePartSpec != null) {
        command.append(" WHERE ");
        List<String> pkv = getPartKVPairStringArray((LinkedHashMap<String, String>) basePartSpec);
        for (int i = 0; i < pkv.size(); i++) {
          String partKV = pkv.get(i);
          command.append(partKV);
          if (i < pkv.size() - 1) {
            command.append(" AND ");
          }
        }
      }
      command.append(" GROUP BY ");
      command.append(indexCols + ", " + VirtualColumn.FILENAME.getName());

      HiveConf builderConf = new HiveConf(getConf(), AggregateIndexHandler.class);
      builderConf.setBoolVar(HiveConf.ConfVars.HIVEMERGEMAPFILES, false);
      builderConf.setBoolVar(HiveConf.ConfVars.HIVEMERGEMAPREDFILES, false);
      builderConf.setBoolVar(HiveConf.ConfVars.HIVEMERGETEZFILES, false);
      Task<?> rootTask = IndexUtils.createRootTask(builderConf, inputs, outputs,
          command, (LinkedHashMap<String, String>) partSpec, indexTableName, dbName);
      super.setStatsDir(builderConf);
      return rootTask;
    }
  }
