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
package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getAllPartitionsOf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDataLocation;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPartColNames;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPartCols;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPartition;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPartitionName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPartitionSpec;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPath;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.isPartitioned;

import java.io.IOException;
import java.time.Instant;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Verify that the information in the metastore matches what is on the
 * filesystem. Return a CheckResult object containing lists of missing and any
 * unexpected tables and partitions.
 */
public class HiveMetaStoreChecker {

  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreChecker.class);

  private final IMetaStoreClient msc;
  private final Configuration conf;
  private final long partitionExpirySeconds;
  private final Interner<Path> pathInterner = Interners.newStrongInterner();

  public HiveMetaStoreChecker(IMetaStoreClient msc, Configuration conf) {
    this(msc, conf, -1);
  }

  public HiveMetaStoreChecker(IMetaStoreClient msc, Configuration conf, long partitionExpirySeconds) {
    super();
    this.msc = msc;
    this.conf = conf;
    this.partitionExpirySeconds = partitionExpirySeconds;
  }

  public IMetaStoreClient getMsc() {
    return msc;
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   *
   * @param catName
   *          name of the catalog, if not specified default catalog will be used.
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
   * @throws MetastoreException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   */
  public void checkMetastore(String catName, String dbName, String tableName,
      List<? extends Map<String, String>> partitions, CheckResult result)
      throws MetastoreException, IOException {

    if (dbName == null || "".equalsIgnoreCase(dbName)) {
      dbName = Warehouse.DEFAULT_DATABASE_NAME;
    }

    try {
      if (tableName == null || "".equals(tableName)) {
        // no table specified, check all tables and all partitions.
        List<String> tables = getMsc().getTables(catName, dbName, ".*");
        for (String currentTableName : tables) {
          checkTable(catName, dbName, currentTableName, null, result);
        }

        findUnknownTables(catName, dbName, tables, result);
      } else if (partitions == null || partitions.isEmpty()) {
        // only one table, let's check all partitions
        checkTable(catName, dbName, tableName, null, result);
      } else {
        // check the specified partitions
        checkTable(catName, dbName, tableName, partitions, result);
      }
      LOG.info("Number of partitionsNotInMs=" + result.getPartitionsNotInMs()
              + ", partitionsNotOnFs=" + result.getPartitionsNotOnFs()
              + ", tablesNotInMs=" + result.getTablesNotInMs()
              + ", tablesNotOnFs=" + result.getTablesNotOnFs()
              + ", expiredPartitions=" + result.getExpiredPartitions());
    } catch (TException e) {
      throw new MetastoreException(e);
    }
  }

  /**
   * Check for table directories that aren't in the metastore.
   *
   * @param catName
   *          name of the catalog, if not specified default catalog will be used.
   * @param dbName
   *          Name of the database
   * @param tables
   *          List of table names
   * @param result
   *          Add any found tables to this
   * @throws IOException
   *           Most likely filesystem related
   * @throws MetaException
   *           Failed to get required information from the metastore.
   * @throws NoSuchObjectException
   *           Failed to get required information from the metastore.
   * @throws TException
   *           Thrift communication error.
   */
  void findUnknownTables(String catName, String dbName, List<String> tables, CheckResult result)
      throws IOException, MetaException, TException {

    Set<Path> dbPaths = new HashSet<Path>();
    Set<String> tableNames = new HashSet<String>(tables);

    for (String tableName : tables) {
      Table table = getMsc().getTable(catName, dbName, tableName);
      // hack, instead figure out a way to get the db paths
      String isExternal = table.getParameters().get("EXTERNAL");
      if (!"TRUE".equalsIgnoreCase(isExternal)) {
        Path tablePath = getPath(table);
        if (tablePath != null) {
          dbPaths.add(tablePath.getParent());
        }
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
   * @param catName
   *          name of the catalog, if not specified default catalog will be used.
   * @param dbName
   *          Name of the database
   * @param tableName
   *          Name of the table
   * @param partitions
   *          Partitions to check, if null or empty get all the partitions.
   * @param result
   *          Result object
   * @throws MetastoreException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   * @throws MetaException
   *           Failed to get required information from the metastore.
   */
  void checkTable(String catName, String dbName, String tableName,
      List<? extends Map<String, String>> partitions, CheckResult result)
      throws MetaException, IOException, MetastoreException {

    Table table;

    try {
      table = getMsc().getTable(catName, dbName, tableName);
    } catch (TException e) {
      result.getTablesNotInMs().add(tableName);
      return;
    }

    PartitionIterable parts;
    boolean findUnknownPartitions = true;

    if (isPartitioned(table)) {
      if (partitions == null || partitions.isEmpty()) {
        int batchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
        if (batchSize > 0) {
          parts = new PartitionIterable(getMsc(), table, batchSize);
        } else {
          List<Partition> loadedPartitions = getAllPartitionsOf(getMsc(), table);
          parts = new PartitionIterable(loadedPartitions);
        }
      } else {
        // we're interested in specific partitions,
        // don't check for any others
        findUnknownPartitions = false;
        List<Partition> loadedPartitions = new ArrayList<>();
        for (Map<String, String> map : partitions) {
          Partition part = getPartition(getMsc(), table, map);
          if (part == null) {
            CheckResult.PartitionResult pr = new CheckResult.PartitionResult();
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
   * @throws MetastoreException
   *           Could not create Partition object
   */
  void checkTable(Table table, PartitionIterable parts,
      boolean findUnknownPartitions, CheckResult result) throws IOException,
    MetastoreException {

    Path tablePath = getPath(table);
    if (tablePath == null) {
      return;
    }
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
      Path partPath = getDataLocation(table, partition);
      if (partPath == null) {
        continue;
      }
      fs = partPath.getFileSystem(conf);
      if (!fs.exists(partPath)) {
        CheckResult.PartitionResult pr = new CheckResult.PartitionResult();
        pr.setPartitionName(getPartitionName(table, partition));
        pr.setTableName(partition.getTableName());
        result.getPartitionsNotOnFs().add(pr);
      }

      if (partitionExpirySeconds > 0) {
        long currentEpochSecs = Instant.now().getEpochSecond();
        long createdTime = partition.getCreateTime();
        long partitionAgeSeconds = currentEpochSecs - createdTime;
        if (partitionAgeSeconds > partitionExpirySeconds) {
          CheckResult.PartitionResult pr = new CheckResult.PartitionResult();
          pr.setPartitionName(getPartitionName(table, partition));
          pr.setTableName(partition.getTableName());
          result.getExpiredPartitions().add(pr);
          if (LOG.isDebugEnabled()) {
            LOG.debug("{}.{}.{}.{} expired. createdAt: {} current: {} age: {}s expiry: {}s", partition.getCatName(),
              partition.getDbName(), partition.getTableName(), pr.getPartitionName(), createdTime, currentEpochSecs,
              partitionAgeSeconds, partitionExpirySeconds);
          }
        }
      }

      for (int i = 0; i < getPartitionSpec(table, partition).size(); i++) {
        Path qualifiedPath = partPath.makeQualified(fs);
        pathInterner.intern(qualifiedPath);
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
   * @throws MetastoreException
   */
  void findUnknownPartitions(Table table, Set<Path> partPaths,
      CheckResult result) throws IOException, MetastoreException {

    Path tablePath = getPath(table);
    if (tablePath == null) {
      return;
    }
    // now check the table folder and see if we find anything
    // that isn't in the metastore
    Set<Path> allPartDirs = new HashSet<Path>();
    checkPartitionDirs(tablePath, allPartDirs, Collections.unmodifiableList(getPartColNames(table)));
    // don't want the table dir
    allPartDirs.remove(tablePath);

    // remove the partition paths we know about
    allPartDirs.removeAll(partPaths);

    Set<String> partColNames = Sets.newHashSet();
    for(FieldSchema fSchema : getPartCols(table)) {
      partColNames.add(fSchema.getName());
    }

    // we should now only have the unexpected folders left
    for (Path partPath : allPartDirs) {
      FileSystem fs = partPath.getFileSystem(conf);
      String partitionName = getPartitionName(fs.makeQualified(tablePath),
          partPath, partColNames);
      LOG.debug("PartitionName: " + partitionName);

      if (partitionName != null) {
        CheckResult.PartitionResult pr = new CheckResult.PartitionResult();
        pr.setPartitionName(partitionName);
        pr.setTableName(table.getTableName());

        result.getPartitionsNotInMs().add(pr);
      }
    }
    LOG.debug("Number of partitions not in metastore : " + result.getPartitionsNotInMs().size());
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
   * @param partColNames
   *          Partition column names
   * @throws IOException
   *           Thrown if we can't get lists from the fs.
   * @throws MetastoreException
   */

  private void checkPartitionDirs(Path basePath, Set<Path> allDirs, final List<String> partColNames) throws IOException, MetastoreException {
    // Here we just reuse the THREAD_COUNT configuration for
    // METASTORE_FS_HANDLER_THREADS_COUNT since this results in better performance
    // The number of missing partitions discovered are later added by metastore using a
    // threadpool of size METASTORE_FS_HANDLER_THREADS_COUNT. If we have different sized
    // pool here the smaller sized pool of the two becomes a bottleneck
    int poolSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT);

    ExecutorService executor;
    if (poolSize <= 1) {
      LOG.debug("Using single-threaded version of MSCK-GetPaths");
      executor = MoreExecutors.newDirectExecutorService();
    } else {
      LOG.debug("Using multi-threaded version of MSCK-GetPaths with number of threads " + poolSize);
      ThreadFactory threadFactory =
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MSCK-GetPaths-%d").build();
      executor = Executors.newFixedThreadPool(poolSize, threadFactory);
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
      this.throwException = "throw".equals(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.MSCK_PATH_VALIDATION));
    }

    @Override
    public Path call() throws Exception {
      return processPathDepthInfo(pd);
    }

    private Path processPathDepthInfo(final PathDepthInfo pd)
        throws IOException, MetastoreException {
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

    private void logOrThrowExceptionWithMsg(String msg) throws MetastoreException {
      if(throwException) {
        throw new MetastoreException(msg);
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
      final FileSystem fs, final List<String> partColNames) throws MetastoreException {
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
      throw new MetastoreException(e.getCause());
    }
  }
}
