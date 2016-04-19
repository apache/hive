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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
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
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Utility class for index support.
 * Currently used for BITMAP and AGGREGATE index
 *
 */
public final class IndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IndexWhereProcessor.class.getName());

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
      ParseContext pctx, List<Index> indexes) throws HiveException {
    Hive hive = Hive.get(pctx.getConf());
    // make sure each partition exists on the index table
    PrunedPartitionList queryPartitionList = pctx.getOpToPartList().get(tableScan);
    Set<Partition> queryPartitions = queryPartitionList.getPartitions();
    if (queryPartitions == null || queryPartitions.isEmpty()) {
      return null;
    }

    for (Partition part : queryPartitions) {
      if (!containsPartition(hive, part, indexes)) {
        return null; // problem if it doesn't contain the partition
      }
    }

    return queryPartitions;
  }

  /**
   * check that every index table contains the given partition and is fresh
   */
  private static boolean containsPartition(Hive hive, Partition part, List<Index> indexes)
      throws HiveException {
    HashMap<String, String> partSpec = part.getSpec();
    if (partSpec.isEmpty()) {
      // empty specs come from non-partitioned tables
      return isIndexTableFresh(hive, indexes, part.getTable());
    }

    for (Index index : indexes) {
      // index.getDbName() is used as a default database, which is database of target table,
      // if index.getIndexTableName() does not contain database name
      String[] qualified = Utilities.getDbTableName(index.getDbName(), index.getIndexTableName());
      Table indexTable = hive.getTable(qualified[0], qualified[1]);
      // get partitions that match the spec
      Partition matchingPartition = hive.getPartition(indexTable, partSpec, false);
      if (matchingPartition == null) {
        LOG.info("Index table " + indexTable + "did not contain built partition that matched " + partSpec);
        return false;
      } else if (!isIndexPartitionFresh(hive, index, part)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check the index partitions on a partitioned table exist and are fresh
   */
  private static boolean isIndexPartitionFresh(Hive hive, Index index,
      Partition part) throws HiveException {
    LOG.info("checking index staleness...");
    try {
      String indexTs = index.getParameters().get(part.getSpec().toString());
      if (indexTs == null) {
        return false;
      }

      FileSystem partFs = part.getDataLocation().getFileSystem(hive.getConf());
      FileStatus[] parts = partFs.listStatus(part.getDataLocation(), FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus status : parts) {
        if (status.getModificationTime() > Long.parseLong(indexTs)) {
          LOG.info("Index is stale on partition '" + part.getName()
              + "'. Modified time (" + status.getModificationTime() + ") for '" + status.getPath()
              + "' is higher than index creation time (" + indexTs + ").");
          return false;
        }
      }
    } catch (IOException e) {
      throw new HiveException("Failed to grab timestamp information from partition '" + part.getName() + "': " + e.getMessage(), e);
    }
    return true;
  }

  /**
   * Check that the indexes on the un-partitioned table exist and are fresh
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
        String indexTs = index.getParameters().get("base_timestamp");
        if (indexTs == null) {
          return false;
        }

        FileSystem srcFs = src.getPath().getFileSystem(hive.getConf());
        FileStatus[] srcs = srcFs.listStatus(src.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        for (FileStatus status : srcs) {
          if (status.getModificationTime() > Long.parseLong(indexTs)) {
            LOG.info("Index is stale on table '" + src.getTableName()
                + "'. Modified time (" + status.getModificationTime() + ") for '" + status.getPath()
                + "' is higher than index creation time (" + indexTs + ").");
            return false;
          }
        }
      } catch (IOException e) {
        throw new HiveException("Failed to grab timestamp information from table '" + src.getTableName() + "': " + e.getMessage(), e);
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

    List<Index> indexesOnTable;
    try {
      indexesOnTable = getAllIndexes(baseTableMetaData, (short) -1); // get all indexes
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

  /**
   * @return List containing Indexes names if there are indexes on this table
   * @throws HiveException
   **/
  public static List<Index> getAllIndexes(Table table, short max) throws HiveException {
    Hive hive = Hive.get();
    return hive.getIndexes(table.getTTable().getDbName(), table.getTTable().getTableName(), max);
  }

  public static Task<?> createRootTask(
      HiveConf builderConf,
      Set<ReadEntity> inputs,
      Set<WriteEntity> outputs,
      StringBuilder command,
      LinkedHashMap<String, String> partSpec,
      String indexTableName,
      String dbName){
    // Don't try to index optimize the query to build the index
    HiveConf.setBoolVar(builderConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
    Driver driver = new Driver(builderConf, SessionState.get().getUserName());
    driver.compile(command.toString(), false);

    Task<?> rootTask = driver.getPlan().getRootTasks().get(0);
    inputs.addAll(driver.getPlan().getInputs());
    outputs.addAll(driver.getPlan().getOutputs());

    IndexMetadataChangeWork indexMetaChange = new IndexMetadataChangeWork(partSpec,
        indexTableName, dbName);
    IndexMetadataChangeTask indexMetaChangeTsk =
        (IndexMetadataChangeTask) TaskFactory.get(indexMetaChange, builderConf);
    indexMetaChangeTsk.setWork(indexMetaChange);
    rootTask.addDependentTask(indexMetaChangeTsk);

    driver.destroy();

    return rootTask;
  }


}
