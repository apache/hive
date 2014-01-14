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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeTask;
import org.apache.hadoop.hive.ql.index.IndexMetadataChangeWork;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.index.IndexWhereProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Utility class for index support.
 * Currently used for BITMAP and AGGREGATE index
 *
 */
public final class IndexUtils {

  private static final Log LOG = LogFactory.getLog(IndexWhereProcessor.class.getName());
  private static final Map<Index, Table> indexToIndexTable = new HashMap<Index, Table>();

  private IndexUtils(){
  }

  /**
   * Check the partitions used by the table scan to make sure they also exist in the
   * index table.
   * @param pctx
   * @param indexes
   * @return partitions used by query.  null if they do not exist in index table
   * @throws HiveException
   */
  public static Set<Partition> checkPartitionsCoveredByIndex(TableScanOperator tableScan,
      ParseContext pctx,
      Map<Table, List<Index>> indexes)
    throws HiveException {
    Hive hive = Hive.get(pctx.getConf());
    // make sure each partition exists on the index table
    PrunedPartitionList queryPartitionList = pctx.getOpToPartList().get(tableScan);
    Set<Partition> queryPartitions = queryPartitionList.getPartitions();
    if (queryPartitions == null || queryPartitions.isEmpty()) {
      return null;
    }

    for (Partition part : queryPartitions) {
      List<Table> sourceIndexTables = getIndexTables(hive, part, indexes);
      if (!containsPartition(hive, part, indexes)) {
        return null; // problem if it doesn't contain the partition
      }
    }

    return queryPartitions;
  }

  /**
   * return index tables associated with a given base table
   */
  private List<Table> getIndexTables(Hive hive, Table table,
      Map<Table, List<Index>> indexes) throws
    HiveException {
    List<Table> indexTables = new ArrayList<Table>();
    if (indexes == null || indexes.get(table) == null) {
      return indexTables;
    }
    for (Index index : indexes.get(table)) {
      Table indexTable = hive.getTable(index.getIndexTableName());
      indexToIndexTable.put(index, indexTable);
      indexTables.add(indexTable);
    }
    return indexTables;
  }

  /**
   * return index tables associated with the base table of the partition
   */
  private static List<Table> getIndexTables(Hive hive, Partition part,
      Map<Table, List<Index>> indexes) throws HiveException {
    List<Table> indexTables = new ArrayList<Table>();
    Table partitionedTable = part.getTable();
    if (indexes == null || indexes.get(partitionedTable) == null) {
      return indexTables;
    }
    for (Index index : indexes.get(partitionedTable)) {
      Table indexTable = hive.getTable(index.getIndexTableName());
      indexToIndexTable.put(index, indexTable);
      indexTables.add(indexTable);
    }
    return indexTables;
  }

  /**
   * check that every index table contains the given partition and is fresh
   */
  private static boolean containsPartition(Hive hive, Partition part,
      Map<Table, List<Index>> indexes)
    throws HiveException {
    HashMap<String, String> partSpec = part.getSpec();

    if (indexes == null || indexes.get(part.getTable()) == null) {
      return false;
    }

    if (partSpec.isEmpty()) {
      // empty specs come from non-partitioned tables
      return isIndexTableFresh(hive, indexes.get(part.getTable()), part.getTable());
    }

    for (Index index : indexes.get(part.getTable())) {
      Table indexTable = indexToIndexTable.get(index);
      // get partitions that match the spec
      List<Partition> matchingPartitions = hive.getPartitions(indexTable, partSpec);
      if (matchingPartitions == null || matchingPartitions.size() == 0) {
        LOG.info("Index table " + indexTable + "did not contain built partition that matched " + partSpec);
        return false;
      } else if (!isIndexPartitionFresh(hive, index, part)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check the index partitions on a parttioned table exist and are fresh
   */
  private static boolean isIndexPartitionFresh(Hive hive, Index index,
      Partition part) throws HiveException {
    LOG.info("checking index staleness...");
    try {
      FileSystem partFs = part.getDataLocation().getFileSystem(hive.getConf());
      FileStatus partFss = partFs.getFileStatus(part.getDataLocation());
      String ts = index.getParameters().get(part.getSpec().toString());
      if (ts == null) {
        return false;
      }
      long indexTs = Long.parseLong(ts);
      LOG.info(partFss.getModificationTime());
      LOG.info(ts);
      if (partFss.getModificationTime() > indexTs) {
        LOG.info("index is stale on the partitions that matched " + part.getSpec());
        return false;
      }
    } catch (IOException e) {
      LOG.info("failed to grab timestamp info");
      throw new HiveException(e);
    }
    return true;
  }

  /**
   * Check that the indexes on the unpartioned table exist and are fresh
   */
  private static boolean isIndexTableFresh(Hive hive, List<Index> indexes, Table src)
    throws HiveException {
    //check that they exist
    if (indexes == null || indexes.size() == 0) {
      return false;
    }
    //check that they are not stale
    for (Index index : indexes) {
      LOG.info("checking index staleness...");
      try {
        FileSystem srcFs = src.getPath().getFileSystem(hive.getConf());
        FileStatus srcFss= srcFs.getFileStatus(src.getPath());
        String ts = index.getParameters().get("base_timestamp");
        if (ts == null) {
          return false;
        }
        long indexTs = Long.parseLong(ts);
        LOG.info(srcFss.getModificationTime());
        LOG.info(ts);
        if (srcFss.getModificationTime() > indexTs) {
          LOG.info("index is stale ");
          return false;
        }
      } catch (IOException e) {
        LOG.info("failed to grab timestamp info");
        throw new HiveException(e);
      }
    }
    return true;
  }


  /**
   * Get a list of indexes on a table that match given types.
   */
  public static List<Index> getIndexes(Table baseTableMetaData, List<String> matchIndexTypes)
    throws SemanticException {
    List<Index> matchingIndexes = new ArrayList<Index>();
    List<Index> indexesOnTable = null;

    try {
      indexesOnTable = baseTableMetaData.getAllIndexes((short) -1); // get all indexes
    } catch (HiveException e) {
      throw new SemanticException("Error accessing metastore", e);
    }

    for (Index index : indexesOnTable) {
      String indexType = index.getIndexHandlerClass();
      if (matchIndexTypes.contains(indexType)) {
        matchingIndexes.add(index);
      }
    }
    return matchingIndexes;
  }


  public static Task<?> createRootTask(HiveConf builderConf, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs, StringBuilder command,
      LinkedHashMap<String, String> partSpec,
      String indexTableName, String dbName){
    // Don't try to index optimize the query to build the index
    HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
    Driver driver = new Driver(builderConf);
    driver.compile(command.toString());

    Task<?> rootTask = driver.getPlan().getRootTasks().get(0);
    inputs.addAll(driver.getPlan().getInputs());
    outputs.addAll(driver.getPlan().getOutputs());

    IndexMetadataChangeWork indexMetaChange = new IndexMetadataChangeWork(partSpec,
        indexTableName, dbName);
    IndexMetadataChangeTask indexMetaChangeTsk = new IndexMetadataChangeTask();
    indexMetaChangeTsk.setWork(indexMetaChange);
    rootTask.addDependentTask(indexMetaChangeTsk);

    return rootTask;
  }


}
