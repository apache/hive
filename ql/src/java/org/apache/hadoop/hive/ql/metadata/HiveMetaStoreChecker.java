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
package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.CheckResult.PartitionResult;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.thrift.TException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Verify that the information in the metastore matches what is on the
 * filesystem. Return a CheckResult object containing lists of missing and any
 * unexpected tables and partitions.
 */
public class HiveMetaStoreChecker {

  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreChecker.class);

  private final Hive hive;
  private final HiveConf conf;

  public HiveMetaStoreChecker(Hive hive) {
    super();
    this.hive = hive;
    conf = hive.getConf();
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   *
   * @param dbName
   *          name of the database, if not specified the default will be used.
   * @param tableName
   *          Table we want to run the check for. If null we'll check all the
   *          tables in the database.
   * @param partitions
   *          List of partition name value pairs, if null or empty check all
   *          partitions
   * @param result
   *          Fill this with the results of the check
   * @throws HiveException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   */
  public void checkMetastore(String dbName, String tableName,
      List<? extends Map<String, String>> partitions, CheckResult result)
      throws HiveException, IOException {

    if (dbName == null || "".equalsIgnoreCase(dbName)) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    try {
      if (tableName == null || "".equals(tableName)) {
        // no table specified, check all tables and all partitions.
        List<String> tables = hive.getTablesForDb(dbName, ".*");
        for (String currentTableName : tables) {
          checkTable(dbName, currentTableName, null, result);
        }

        findUnknownTables(dbName, tables, result);
      } else if (partitions == null || partitions.isEmpty()) {
        // only one table, let's check all partitions
        checkTable(dbName, tableName, null, result);
      } else {
        // check the specified partitions
        checkTable(dbName, tableName, partitions, result);
      }
      LOG.info("Number of partitionsNotInMs=" + result.getPartitionsNotInMs()
              + ", partitionsNotOnFs=" + result.getPartitionsNotOnFs()
              + ", tablesNotInMs=" + result.getTablesNotInMs()
              + ", tablesNotOnFs=" + result.getTablesNotOnFs());
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Check for table directories that aren't in the metastore.
   *
   * @param dbName
   *          Name of the database
   * @param tables
   *          List of table names
   * @param result
   *          Add any found tables to this
   * @throws HiveException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   * @throws MetaException
   *           Failed to get required information from the metastore.
   * @throws NoSuchObjectException
   *           Failed to get required information from the metastore.
   * @throws TException
   *           Thrift communication error.
   */
  void findUnknownTables(String dbName, List<String> tables, CheckResult result)
      throws IOException, MetaException, TException, HiveException {

    Set<Path> dbPaths = new HashSet<Path>();
    Set<String> tableNames = new HashSet<String>(tables);

    for (String tableName : tables) {
      Table table = hive.getTable(dbName, tableName);
      // hack, instead figure out a way to get the db paths
      String isExternal = table.getParameters().get("EXTERNAL");
      if (isExternal == null || !"TRUE".equalsIgnoreCase(isExternal)) {
        dbPaths.add(table.getPath().getParent());
      }
    }

    for (Path dbPath : dbPaths) {
      FileSystem fs = dbPath.getFileSystem(conf);
      FileStatus[] statuses = fs.listStatus(dbPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus status : statuses) {

        if (status.isDir() && !tableNames.contains(status.getPath().getName())) {

          result.getTablesNotInMs().add(status.getPath().getName());
        }
      }
    }
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   *
   * @param dbName
   *          Name of the database
   * @param tableName
   *          Name of the table
   * @param partitions
   *          Partitions to check, if null or empty get all the partitions.
   * @param result
   *          Result object
   * @throws HiveException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   * @throws MetaException
   *           Failed to get required information from the metastore.
   */
  void checkTable(String dbName, String tableName,
      List<? extends Map<String, String>> partitions, CheckResult result)
      throws MetaException, IOException, HiveException {

    Table table = null;

    try {
      table = hive.getTable(dbName, tableName);
    } catch (HiveException e) {
      result.getTablesNotInMs().add(tableName);
      return;
    }

    List<Partition> parts = new ArrayList<Partition>();
    boolean findUnknownPartitions = true;

    if (table.isPartitioned()) {
      if (partitions == null || partitions.isEmpty()) {
        PrunedPartitionList prunedPartList =
        PartitionPruner.prune(table, null, conf, toString(), null);
        // no partitions specified, let's get all
        parts.addAll(prunedPartList.getPartitions());
      } else {
        // we're interested in specific partitions,
        // don't check for any others
        findUnknownPartitions = false;
        for (Map<String, String> map : partitions) {
          Partition part = hive.getPartition(table, map, false);
          if (part == null) {
            PartitionResult pr = new PartitionResult();
            pr.setTableName(tableName);
            pr.setPartitionName(Warehouse.makePartPath(map));
            result.getPartitionsNotInMs().add(pr);
          } else {
            parts.add(part);
          }
        }
      }
    }

    checkTable(table, parts, findUnknownPartitions, result);
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   *
   * @param table
   *          Table to check
   * @param parts
   *          Partitions to check
   * @param result
   *          Result object
   * @param findUnknownPartitions
   *          Should we try to find unknown partitions?
   * @throws IOException
   *           Could not get information from filesystem
   * @throws HiveException
   *           Could not create Partition object
   */
  void checkTable(Table table, List<Partition> parts,
      boolean findUnknownPartitions, CheckResult result) throws IOException,
      HiveException {

    Path tablePath = table.getPath();
    FileSystem fs = tablePath.getFileSystem(conf);
    if (!fs.exists(tablePath)) {
      result.getTablesNotOnFs().add(table.getTableName());
      return;
    }

    Set<Path> partPaths = new HashSet<Path>();

    // check that the partition folders exist on disk
    for (Partition partition : parts) {
      if (partition == null) {
        // most likely the user specified an invalid partition
        continue;
      }
      Path partPath = partition.getDataLocation();
      fs = partPath.getFileSystem(conf);
      if (!fs.exists(partPath)) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionName(partition.getName());
        pr.setTableName(partition.getTable().getTableName());
        result.getPartitionsNotOnFs().add(pr);
      }

      for (int i = 0; i < partition.getSpec().size(); i++) {
        partPaths.add(partPath.makeQualified(fs));
        partPath = partPath.getParent();
      }
    }

    if (findUnknownPartitions) {
      findUnknownPartitions(table, partPaths, result);
    }
  }

  /**
   * Find partitions on the fs that are unknown to the metastore.
   *
   * @param table
   *          Table where the partitions would be located
   * @param partPaths
   *          Paths of the partitions the ms knows about
   * @param result
   *          Result object
   * @throws IOException
   *           Thrown if we fail at fetching listings from the fs.
   * @throws HiveException 
   */
  void findUnknownPartitions(Table table, Set<Path> partPaths,
      CheckResult result) throws IOException, HiveException {

    Path tablePath = table.getPath();
    // now check the table folder and see if we find anything
    // that isn't in the metastore
    Set<Path> allPartDirs = new HashSet<Path>();
    checkPartitionDirs(tablePath, allPartDirs, table.getPartCols().size());
    // don't want the table dir
    allPartDirs.remove(tablePath);

    // remove the partition paths we know about
    allPartDirs.removeAll(partPaths);

    Set<String> partColNames = Sets.newHashSet();
    for(FieldSchema fSchema : table.getPartCols()) {
      partColNames.add(fSchema.getName());
    }

    // we should now only have the unexpected folders left
    for (Path partPath : allPartDirs) {
      FileSystem fs = partPath.getFileSystem(conf);
      String partitionName = getPartitionName(fs.makeQualified(tablePath),
          partPath, partColNames);
      LOG.debug("PartitionName: " + partitionName);

      if (partitionName != null) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionName(partitionName);
        pr.setTableName(table.getTableName());

        result.getPartitionsNotInMs().add(pr);
      }
    }
    LOG.debug("Number of partitions not in metastore : " + result.getPartitionsNotInMs().size());
  }

  /**
   * Get the partition name from the path.
   *
   * @param tablePath
   *          Path of the table.
   * @param partitionPath
   *          Path of the partition.
   * @param partCols
   *          Set of partition columns from table definition
   * @return Partition name, for example partitiondate=2008-01-01
   */
  static String getPartitionName(Path tablePath, Path partitionPath,
      Set<String> partCols) {
    String result = null;
    Path currPath = partitionPath;
    LOG.debug("tablePath:" + tablePath + ", partCols: " + partCols);

    while (currPath != null && !tablePath.equals(currPath)) {
      // format: partition=p_val
      // Add only when table partition colName matches
      String[] parts = currPath.getName().split("=");
      if (parts != null && parts.length > 0) {
        if (parts.length != 2) {
          LOG.warn(currPath.getName() + " is not a valid partition name");
          return result;
        }

        String partitionName = parts[0];
        if (partCols.contains(partitionName)) {
          if (result == null) {
            result = currPath.getName();
          } else {
            result = currPath.getName() + Path.SEPARATOR + result;
          }
        }
      }
      currPath = currPath.getParent();
      LOG.debug("currPath=" + currPath);
    }
    return result;
  }

  /**
   * Assume that depth is 2, i.e., partition columns are a and b
   * tblPath/a=1  => throw exception
   * tblPath/a=1/file => throw exception
   * tblPath/a=1/b=2/file => return a=1/b=2
   * tblPath/a=1/b=2/c=3 => return a=1/b=2
   * tblPath/a=1/b=2/c=3/file => return a=1/b=2
   *
   * @param basePath
   *          Start directory
   * @param allDirs
   *          This set will contain the leaf paths at the end.
   * @param maxDepth
   *          Specify how deep the search goes.
   * @throws IOException
   *           Thrown if we can't get lists from the fs.
   * @throws HiveException
   */

  private void checkPartitionDirs(Path basePath, Set<Path> allDirs, int maxDepth) throws IOException, HiveException {
    ConcurrentLinkedQueue<Path> basePaths = new ConcurrentLinkedQueue<>();
    basePaths.add(basePath);
    Set<Path> dirSet = Collections.newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
    // Here we just reuse the THREAD_COUNT configuration for
    // HIVE_MOVE_FILES_THREAD_COUNT
    int poolSize = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 15);

    // Check if too low config is provided for move files. 2x CPU is reasonable max count.
    poolSize = poolSize == 0 ? poolSize : Math.max(poolSize,
        Runtime.getRuntime().availableProcessors() * 2);

    // Fixed thread pool on need basis
    final ThreadPoolExecutor pool = poolSize > 0 ? (ThreadPoolExecutor)
        Executors.newFixedThreadPool(poolSize,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MSCK-GetPaths-%d").build()) : null;

    if (pool == null) {
      LOG.debug("Not-using threaded version of MSCK-GetPaths");
    } else {
      LOG.debug("Using threaded version of MSCK-GetPaths with number of threads "
          + pool.getMaximumPoolSize());
    }
    checkPartitionDirs(pool, basePaths, dirSet, basePath.getFileSystem(conf), maxDepth, maxDepth);
    if (pool != null) {
      pool.shutdown();
    }
    allDirs.addAll(dirSet);
  }

  // process the basePaths in parallel and then the next level of basePaths
  private void checkPartitionDirs(final ThreadPoolExecutor pool,
      final ConcurrentLinkedQueue<Path> basePaths, final Set<Path> allDirs,
      final FileSystem fs, final int depth, final int maxDepth) throws IOException, HiveException {
    final ConcurrentLinkedQueue<Path> nextLevel = new ConcurrentLinkedQueue<>();

    // Check if thread pool can be used.
    boolean useThreadPool = false;
    if (pool != null) {
      synchronized (pool) {
        // In case of recursive calls, it is possible to deadlock with TP. Check TP usage here.
        if (pool.getActiveCount() < pool.getMaximumPoolSize()) {
          useThreadPool = true;
        }

        if (!useThreadPool) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not using threadPool as active count:" + pool.getActiveCount()
                + ", max:" + pool.getMaximumPoolSize());
          }
        }
      }
    }

    if (null == pool || !useThreadPool) {
      for (final Path path : basePaths) {
        FileStatus[] statuses = fs.listStatus(path, FileUtils.HIDDEN_FILES_PATH_FILTER);
        boolean fileFound = false;
        for (FileStatus status : statuses) {
          if (status.isDirectory()) {
            nextLevel.add(status.getPath());
          } else {
            fileFound = true;
          }
        }
        if (depth != 0) {
          // we are in the middle of the search and we find a file
          if (fileFound) {
            if ("throw".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION))) {
              throw new HiveException(
                  "MSCK finds a file rather than a folder when it searches for " + path.toString());
            } else {
              LOG.warn("MSCK finds a file rather than a folder when it searches for "
                  + path.toString());
            }
          }
          if (!nextLevel.isEmpty()) {
            checkPartitionDirs(pool, nextLevel, allDirs, fs, depth - 1, maxDepth);
          } else if (depth != maxDepth) {
            // since nextLevel is empty, we are missing partition columns.
            if ("throw".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION))) {
              throw new HiveException("MSCK is missing partition columns under " + path.toString());
            } else {
              LOG.warn("MSCK is missing partition columns under " + path.toString());
            }
          }
        } else {
          allDirs.add(path);
        }
      }
    } else {
      final List<Future<Void>> futures = new LinkedList<>();
      for (final Path path : basePaths) {
        futures.add(pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            FileStatus[] statuses = fs.listStatus(path, FileUtils.HIDDEN_FILES_PATH_FILTER);
            boolean fileFound = false;
            for (FileStatus status : statuses) {
              if (status.isDirectory()) {
                nextLevel.add(status.getPath());
              } else {
                fileFound = true;
              }
            }
            if (depth != 0) {
              // we are in the middle of the search and we find a file
              if (fileFound) {
                if ("throw".equals(HiveConf.getVar(conf,
                    HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION))) {
                  throw new HiveException(
                      "MSCK finds a file rather than a folder when it searches for "
                          + path.toString());
                } else {
                  LOG.warn("MSCK finds a file rather than a folder when it searches for "
                      + path.toString());
                }
              }
              if (!nextLevel.isEmpty()) {
                checkPartitionDirs(pool, nextLevel, allDirs, fs, depth - 1, maxDepth);
              } else if (depth != maxDepth) {
                // since nextLevel is empty, we are missing partition columns.
                if ("throw".equals(HiveConf.getVar(conf,
                    HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION))) {
                  throw new HiveException("MSCK is missing partition columns under "
                      + path.toString());
                } else {
                  LOG.warn("MSCK is missing partition columns under " + path.toString());
                }
              }
            } else {
              allDirs.add(path);
            }
            return null;
          }
        }));
      }
      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          LOG.error(e.getMessage());
          pool.shutdownNow();
          throw new HiveException(e.getCause());
        }
      }
      if (!nextLevel.isEmpty() && depth != 0) {
        checkPartitionDirs(pool, nextLevel, allDirs, fs, depth - 1, maxDepth);
      }
    }
  }
}
