/*
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.CheckResult.PartitionResult;
import org.apache.thrift.TException;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Verify that the information in the metastore matches what is on the
 * filesystem. Return a CheckResult object containing lists of missing and any
 * unexpected tables and partitions.
 */
public class HiveMetaStoreChecker {

  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreChecker.class);
  public static final String CLASS_NAME = HiveMetaStoreChecker.class.getName();

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
      dbName = Warehouse.DEFAULT_DATABASE_NAME;
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

    PartitionIterable parts;
    boolean findUnknownPartitions = true;

    if (table.isPartitioned()) {
      if (partitions == null || partitions.isEmpty()) {
        String mode = HiveConf.getVar(conf, ConfVars.HIVEMAPREDMODE, (String) null);
        if ("strict".equalsIgnoreCase(mode)) {
          parts = new PartitionIterable(hive, table, null, conf.getIntVar(
              HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
        } else {
          List<Partition> loadedPartitions = new ArrayList<>();
          PerfLogger perfLogger = SessionState.getPerfLogger();
          perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
          loadedPartitions.addAll(hive.getAllPartitionsOf(table));
          perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
          parts = new PartitionIterable(loadedPartitions);
        }
      } else {
        // we're interested in specific partitions,
        // don't check for any others
        findUnknownPartitions = false;
        List<Partition> loadedPartitions = new ArrayList<>();
        for (Map<String, String> map : partitions) {
          Partition part = hive.getPartition(table, map, false);
          if (part == null) {
            PartitionResult pr = new PartitionResult();
            pr.setTableName(tableName);
            pr.setPartitionName(Warehouse.makePartPath(map));
            result.getPartitionsNotInMs().add(pr);
          } else {
            loadedPartitions.add(part);
          }
        }
        parts = new PartitionIterable(loadedPartitions);
      }
    } else {
      parts = new PartitionIterable(Collections.<Partition>emptyList());
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
  void checkTable(Table table, PartitionIterable parts,
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
        Path qualifiedPath = partPath.makeQualified(fs);
        StringInternUtils.internUriStringsInPath(qualifiedPath);
        partPaths.add(qualifiedPath);
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
    checkPartitionDirs(tablePath, allPartDirs, Collections.unmodifiableList(table.getPartColNames()));
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
   * @param list
   *          Specify how deep the search goes.
   * @throws IOException
   *           Thrown if we can't get lists from the fs.
   * @throws HiveException
   */

  private void checkPartitionDirs(Path basePath, Set<Path> allDirs, final List<String> partColNames) throws IOException, HiveException {
    // Here we just reuse the THREAD_COUNT configuration for
    // METASTORE_FS_HANDLER_THREADS_COUNT since this results in better performance
    // The number of missing partitions discovered are later added by metastore using a
    // threadpool of size METASTORE_FS_HANDLER_THREADS_COUNT. If we have different sized
    // pool here the smaller sized pool of the two becomes a bottleneck
    int poolSize = conf.getInt(ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT.varname, 15);

    ExecutorService executor;
    if (poolSize <= 1) {
      LOG.debug("Using single-threaded version of MSCK-GetPaths");
      executor = MoreExecutors.newDirectExecutorService();
    } else {
      LOG.debug("Using multi-threaded version of MSCK-GetPaths with number of threads " + poolSize);
      ThreadFactory threadFactory =
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MSCK-GetPaths-%d").build();
      executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize, threadFactory);
    }
    checkPartitionDirs(executor, basePath, allDirs, basePath.getFileSystem(conf), partColNames);

    executor.shutdown();
  }

  private final class PathDepthInfoCallable implements Callable<Path> {
    private final List<String> partColNames;
    private final FileSystem fs;
    private final ConcurrentLinkedQueue<PathDepthInfo> pendingPaths;
    private final boolean throwException;
    private final PathDepthInfo pd;

    private PathDepthInfoCallable(PathDepthInfo pd, List<String> partColNames, FileSystem fs,
        ConcurrentLinkedQueue<PathDepthInfo> basePaths) {
      this.partColNames = partColNames;
      this.pd = pd;
      this.fs = fs;
      this.pendingPaths = basePaths;
      this.throwException = "throw"
      .equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_MSCK_PATH_VALIDATION));
    }

    @Override
    public Path call() throws Exception {
      return processPathDepthInfo(pd);
    }

    private Path processPathDepthInfo(final PathDepthInfo pd)
        throws IOException, HiveException, InterruptedException {
      final Path currentPath = pd.p;
      final int currentDepth = pd.depth;
      FileStatus[] fileStatuses = fs.listStatus(currentPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      // found no files under a sub-directory under table base path; it is possible that the table
      // is empty and hence there are no partition sub-directories created under base path
      if (fileStatuses.length == 0 && currentDepth > 0 && currentDepth < partColNames.size()) {
        // since maxDepth is not yet reached, we are missing partition
        // columns in currentPath
        logOrThrowExceptionWithMsg(
            "MSCK is missing partition columns under " + currentPath.toString());
      } else {
        // found files under currentPath add them to the queue if it is a directory
        for (FileStatus fileStatus : fileStatuses) {
          if (!fileStatus.isDirectory() && currentDepth < partColNames.size()) {
            // found a file at depth which is less than number of partition keys
            logOrThrowExceptionWithMsg(
                "MSCK finds a file rather than a directory when it searches for "
                    + fileStatus.getPath().toString());
          } else if (fileStatus.isDirectory() && currentDepth < partColNames.size()) {
            // found a sub-directory at a depth less than number of partition keys
            // validate if the partition directory name matches with the corresponding
            // partition colName at currentDepth
            Path nextPath = fileStatus.getPath();
            String[] parts = nextPath.getName().split("=");
            if (parts.length != 2) {
              logOrThrowExceptionWithMsg("Invalid partition name " + nextPath);
            } else if (!parts[0].equalsIgnoreCase(partColNames.get(currentDepth))) {
              logOrThrowExceptionWithMsg(
                  "Unexpected partition key " + parts[0] + " found at " + nextPath);
            } else {
              // add sub-directory to the work queue if maxDepth is not yet reached
              pendingPaths.add(new PathDepthInfo(nextPath, currentDepth + 1));
            }
          }
        }
        if (currentDepth == partColNames.size()) {
          return currentPath;
        }
      }
      return null;
    }

    private void logOrThrowExceptionWithMsg(String msg) throws HiveException {
      if(throwException) {
        throw new HiveException(msg);
      } else {
        LOG.warn(msg);
      }
    }
  }

  private static class PathDepthInfo {
    private final Path p;
    private final int depth;
    PathDepthInfo(Path p, int depth) {
      this.p = p;
      this.depth = depth;
    }
  }

  private void checkPartitionDirs(final ExecutorService executor,
      final Path basePath, final Set<Path> result,
      final FileSystem fs, final List<String> partColNames) throws HiveException {
    try {
      Queue<Future<Path>> futures = new LinkedList<Future<Path>>();
      ConcurrentLinkedQueue<PathDepthInfo> nextLevel = new ConcurrentLinkedQueue<>();
      nextLevel.add(new PathDepthInfo(basePath, 0));
      //Uses level parallel implementation of a bfs. Recursive DFS implementations
      //have a issue where the number of threads can run out if the number of
      //nested sub-directories is more than the pool size.
      //Using a two queue implementation is simpler than one queue since then we will
      //have to add the complex mechanisms to let the free worker threads know when new levels are
      //discovered using notify()/wait() mechanisms which can potentially lead to bugs if
      //not done right
      while(!nextLevel.isEmpty()) {
        ConcurrentLinkedQueue<PathDepthInfo> tempQueue = new ConcurrentLinkedQueue<>();
        //process each level in parallel
        while(!nextLevel.isEmpty()) {
          futures.add(
              executor.submit(new PathDepthInfoCallable(nextLevel.poll(), partColNames, fs, tempQueue)));
        }
        while(!futures.isEmpty()) {
          Path p = futures.poll().get();
          if (p != null) {
            result.add(p);
          }
        }
        //update the nextlevel with newly discovered sub-directories from the above
        nextLevel = tempQueue;
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error(e.getMessage());
      executor.shutdownNow();
      throw new HiveException(e.getCause());
    }
  }
}
