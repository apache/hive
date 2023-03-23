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

import static org.apache.hadoop.hive.common.AcidConstants.BASE_PREFIX;
import static org.apache.hadoop.hive.common.AcidConstants.DELETE_DELTA_PREFIX;
import static org.apache.hadoop.hive.common.AcidConstants.DELTA_PREFIX;
import static org.apache.hadoop.hive.common.AcidConstants.VISIBILITY_PREFIX;
import static org.apache.hadoop.hive.metastore.PartFilterExprUtil.createExpressionProxy;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getAllPartitionsOf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getDataLocation;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartColNames;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartCols;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartitionListByFilterExp;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartitionName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartitionColtoTypeMap;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPartitionsByProjectSpec;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.getPath;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isPartitioned;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionProjectionsSpecBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final PathFilter HIDDEN_FILES_PATH_FILTER =
      p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

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
   * @param filterExp
   *          Filter expression which is used to prune th partition from the
   *          metastore and FileSystem.
   * @param table
   * @return Results of the check
   * @throws MetastoreException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   */
  public CheckResult checkMetastore(String catName, String dbName, String tableName,
                             byte[] filterExp, Table table)
      throws MetastoreException, IOException {
    CheckResult result = new CheckResult();
    if (dbName == null || "".equalsIgnoreCase(dbName)) {
      dbName = Warehouse.DEFAULT_DATABASE_NAME;
    }

    try {
      if (tableName == null || "".equals(tableName)) {
        // TODO: I do not think this is used by anything other than tests
        // no table specified, check all tables and all partitions.
        List<String> tables = new ArrayList<>();
        try{
          tables = getMsc().getTables(catName, dbName, ".*");
        }catch(UnknownDBException ex){
          //ignore database exception.
        }
        for (String currentTableName : tables) {
          checkTable(catName, dbName, currentTableName, null, null, result);
        }

        findUnknownTables(catName, dbName, tables, result);
      } else if (filterExp != null) {
        // check for specified partitions which matches filter expression
        checkTable(catName, dbName, tableName, filterExp, table, result);
      } else {
        // only one table, let's check all partitions
        checkTable(catName, dbName, tableName, null, table, result);
      }

      LOG.info("Number of partitionsNotInMs=" + result.getPartitionsNotInMs()
              + ", partitionsNotOnFs=" + result.getPartitionsNotOnFs()
              + ", tablesNotInMs=" + result.getTablesNotInMs()
              + ", tablesNotOnFs=" + result.getTablesNotOnFs()
              + ", expiredPartitions=" + result.getExpiredPartitions());
    } catch (TException e) {
      throw new MetastoreException(e);
    }
    return result;
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

    Set<Path> dbPaths = new HashSet<>();
    Set<String> tableNames = new HashSet<>(tables);

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

        if (status.isDirectory() && !tableNames.contains(status.getPath().getName())) {

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
   * @param filterExp
   *          Filter expression which is used to prune th partition from the
   *          metastore and FileSystem.
   * @param table Table we want to run the check for.
   * @param result
   *          Result object
   * @throws MetastoreException
   *           Failed to get required information from the metastore.
   * @throws IOException
   *           Most likely filesystem related
   * @throws MetaException
   *           Failed to get required information from the metastore.
   */
  void checkTable(String catName, String dbName, String tableName, byte[] filterExp, Table table, CheckResult result)
      throws MetaException, IOException, MetastoreException {

    if (table == null) {
      try {
        table = getMsc().getTable(catName, dbName, tableName);
      } catch (TException e) {
        result.getTablesNotInMs().add(tableName);
        return;
      }
    }

    PartitionIterable parts;

    if (isPartitioned(table)) {
      if (filterExp != null) {
        List<Partition> results = new ArrayList<>();
        getPartitionListByFilterExp(getMsc(), table, filterExp,
            MetastoreConf.getVar(conf, MetastoreConf.ConfVars.DEFAULTPARTITIONNAME), results);
        parts = new PartitionIterable(results);
      } else {
        GetPartitionsRequest request = new GetPartitionsRequest(table.getDbName(), table.getTableName(),
            null, null);
        request.setProjectionSpec(new GetPartitionProjectionsSpecBuilder().addProjectField("sd.location")
            .addProjectField("createTime").addProjectField("tableName")
            .addProjectField("values").build());
        request.setCatName(table.getCatName());
        int batchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
        if (batchSize > 0) {
          parts = new PartitionIterable(getMsc(), table, batchSize).withProjectSpec(request);
        } else {
          parts = new PartitionIterable(getPartitionsByProjectSpec(msc, request));
        }
      }
    } else {
      parts = new PartitionIterable(Collections.emptyList());
    }

    checkTable(table, parts, filterExp, result);
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
   * @param filterExp
   *          Filter expression which is used to prune th partition from the
   *          metastore and FileSystem.
   * @throws IOException
   *           Could not get information from filesystem
   * @throws MetastoreException
   *           Could not create Partition object
   */
  void checkTable(Table table, PartitionIterable parts, byte[] filterExp, CheckResult result) throws IOException,
    MetastoreException, MetaException {

    Path tablePath = getPath(table);
    if (tablePath == null) {
      return;
    }
    FileSystem fs = tablePath.getFileSystem(conf);
    if (!fs.exists(tablePath)) {
      result.getTablesNotOnFs().add(table.getTableName());
      return;
    }

    // now check the table folder and see if we find anything
    // that isn't in the metastore
    Set<Path> allPartDirs = new HashSet<>();
    List<FieldSchema> partColumns = table.getPartitionKeys();
    checkPartitionDirs(tablePath, allPartDirs, Collections.unmodifiableList(getPartColNames(table)));
    String tablePathStr = tablePath.toString();
    int tablePathLength = tablePathStr.length();

    if (filterExp != null) {
      PartitionExpressionProxy expressionProxy = createExpressionProxy(conf);
      List<String> partitions = new ArrayList<>();
      Set<Path> partDirs = new HashSet<Path>();
      boolean tablePathStrEndsWith = tablePathStr.endsWith("/");
      int tablePathStrLen = tablePathStr.endsWith("/") ? tablePathStr.length() : tablePathStr.length() + 1;
      allPartDirs.stream().forEach(path -> partitions.add(path.toString().substring(tablePathStrLen)));

      // Remove all partition paths which does not matches the filter expression.
      expressionProxy.filterPartitionsByExpr(partColumns, filterExp,
              conf.get(MetastoreConf.ConfVars.DEFAULTPARTITIONNAME.getVarname()), partitions);

      // now the partition list will contain all the paths that matches the filter expression.
      // add them back to partDirs.
      for (String path : partitions) {
        partDirs.add(new Path(tablePath, path));
      }
      allPartDirs = partDirs;
    }

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

      CheckResult.PartitionResult prFromMetastore = new CheckResult.PartitionResult();
      prFromMetastore.setPartitionName(getPartitionName(table, partition));
      prFromMetastore.setTableName(partition.getTableName());
      if (allPartDirs.remove(partPath)) {
        result.getCorrectPartitions().add(prFromMetastore);
      } else {
        // There can be edge case where user can define partition directory outside of table directory
        // to avoid eviction of such partitions
        // we check existence of partition path which are not in table directory
        if (!partPath.toString().contains(tablePathStr)) {
          if (!fs.exists(partPath)) {
            result.getPartitionsNotOnFs().add(prFromMetastore);
          } else {
            result.getCorrectPartitions().add(prFromMetastore);
          }
        } else {
          // If Partition Path contains table path, we assume to be non-existent partition since
          // Partition spec has to be in format FS://<TablePath>/<PartKeyName>=<PartValue>
          // otherwise partition discovery may fail.
          result.getPartitionsNotOnFs().add(prFromMetastore);
        }
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
            LOG.debug("{}.{}.{}.{} expired. createdAt: {} current: {} age: {}s expiry: {}s", table.getCatName(),
                table.getDbName(), partition.getTableName(), pr.getPartitionName(), createdTime, currentEpochSecs,
                partitionAgeSeconds, partitionExpirySeconds);
          }
        }
      }
    }

    findUnknownPartitions(table, allPartDirs, result);

    if (!isPartitioned(table) && TxnUtils.isTransactionalTable(table)) {
      // Check for writeIds in the table directory
      CheckResult.PartitionResult tableResult = new CheckResult.PartitionResult();
      setMaxTxnAndWriteIdFromPartition(tablePath, tableResult);
      result.setMaxWriteId(tableResult.getMaxWriteId());
      result.setMaxTxnId(tableResult.getMaxTxnId());
    }
  }

  /**
   * Add partitions on the fs that are unknown to the metastore.
   *
   * @param table
   *          Table where the partitions would be located
   * @param missingPartDirs
   *          Paths of the partitions that ms does not know about
   * @param result
   *          Result object
   * @throws IOException
   *           Thrown if we fail at fetching listings from the fs.
   * @throws MetastoreException ex
   */
  void findUnknownPartitions(Table table, Set<Path> missingPartDirs,
      CheckResult result) throws IOException, MetastoreException, MetaException {

    Path tablePath = getPath(table);
    if (tablePath == null) {
      return;
    }
    boolean transactionalTable = TxnUtils.isTransactionalTable(table);

    Set<String> partColNames = Sets.newHashSet();
    for(FieldSchema fSchema : getPartCols(table)) {
      partColNames.add(fSchema.getName());
    }

    Map<String, String> partitionColToTypeMap = getPartitionColtoTypeMap(table.getPartitionKeys());
    // we should now only have the unexpected folders left
    for (Path partPath : missingPartDirs) {
      FileSystem fs = partPath.getFileSystem(conf);
      String partitionName = getPartitionName(fs.makeQualified(tablePath),
          partPath, partColNames, partitionColToTypeMap);
      LOG.debug("PartitionName: " + partitionName);

      if (partitionName != null) {
        CheckResult.PartitionResult pr = new CheckResult.PartitionResult();
        pr.setPartitionName(partitionName);
        pr.setTableName(table.getTableName());
        // Also set the correct partition path here as creating path from Warehouse.makePartPath will always return
        // lowercase keys/path. Even if we add the new partition with lowerkeys, get queries on such partition
        // will not return any results.
        pr.setPath(partPath);

        // Check if partition already exists. No need to check for those partition which are present in db
        // but no in fs as msck will override the partition location in db
        if (result.getCorrectPartitions().contains(pr)) {
          String msg = "The partition '" + pr.toString() + "' already exists for table" + table.getTableName();
          throw new MetastoreException(msg);
        } else if (result.getPartitionsNotInMs().contains(pr)) {
          String msg = "Found two paths for same partition '" + pr.toString() + "' for table " + table.getTableName();
          throw new MetastoreException(msg);
        }
        if (transactionalTable) {
          setMaxTxnAndWriteIdFromPartition(partPath, pr);
        }
        result.getPartitionsNotInMs().add(pr);
        if (result.getPartitionsNotOnFs().contains(pr)) {
          result.getPartitionsNotOnFs().remove(pr);
        }
      }
    }
    LOG.debug("Number of partitions not in metastore : " + result.getPartitionsNotInMs().size());
  }

  /**
   * Calculate the maximum seen writeId from the acid directory structure
   * @param partPath Path of the partition directory
   * @param res Partition result to write the max ids
   * @throws IOException ex
   */
  private void setMaxTxnAndWriteIdFromPartition(Path partPath, CheckResult.PartitionResult res) throws IOException {
    FileSystem fs = partPath.getFileSystem(conf);
    FileStatus[] deltaOrBaseFiles = fs.listStatus(partPath, HIDDEN_FILES_PATH_FILTER);

    // Read the writeIds from every base and delta directory and find the max
    long maxWriteId = 0L;
    long maxVisibilityId = 0L;
    for (FileStatus fileStatus : deltaOrBaseFiles) {
      if (!fileStatus.isDirectory()) {
        continue;
      }
      long writeId = 0L;
      long visibilityId = 0L;
      String folder = fileStatus.getPath().getName();
      String visParts[] = folder.split(VISIBILITY_PREFIX);
      if (visParts.length > 1) {
        visibilityId = Long.parseLong(visParts[1]);
        folder = visParts[0];
      }
      if (folder.startsWith(BASE_PREFIX)) {
        writeId = Long.parseLong(folder.substring(BASE_PREFIX.length()));
      } else if (folder.startsWith(DELTA_PREFIX) || folder.startsWith(DELETE_DELTA_PREFIX)) {
        // See AcidUtils.parseDelta
        boolean isDeleteDelta = folder.startsWith(DELETE_DELTA_PREFIX);
        String rest = folder.substring((isDeleteDelta ? DELETE_DELTA_PREFIX : DELTA_PREFIX).length());
        String[] nameParts = rest.split("_");
        // We always want the second part (it is either the same or greater if it is a compacted delta)
        writeId = Long.parseLong(nameParts.length > 1 ? nameParts[1] : nameParts[0]);
      }
      if (writeId > maxWriteId) {
        maxWriteId = writeId;
      }
      if (visibilityId > maxVisibilityId) {
        maxVisibilityId = visibilityId;
      }
    }
    LOG.debug("Max writeId {}, max txnId {} found in partition {}", maxWriteId, maxVisibilityId,
        partPath.toUri().toString());
    res.setMaxWriteId(maxWriteId);
    res.setMaxTxnId(maxVisibilityId);
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
   * @throws MetastoreException ex
   */

  private void checkPartitionDirs(Path basePath, Set<Path> allDirs, final List<String> partColNames)
      throws IOException, MetastoreException {
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
      if (currentDepth == partColNames.size()) {
        return currentPath;
      }
      List<FileStatus> fileStatuses = new ArrayList<>();
      RemoteIterator<FileStatus> fileIterator =
          RemoteIterators.filteringRemoteIterator(fs.listStatusIterator(currentPath),
            fileStatus -> HIDDEN_FILES_PATH_FILTER.accept(fileStatus.getPath()));
      while (fileIterator.hasNext()) {
        fileStatuses.add(fileIterator.next());
      }
      // found no files under a sub-directory under table base path; it is possible that the table
      // is empty and hence there are no partition sub-directories created under base path
      if (fileStatuses.size() == 0 && currentDepth > 0) {
        // since maxDepth is not yet reached, we are missing partition
        // columns in currentPath
        logOrThrowExceptionWithMsg(
            "MSCK is missing partition columns under " + currentPath.toString());
      } else {
        // found files under currentPath add them to the queue if it is a directory
        for (FileStatus fileStatus : fileStatuses) {
          if (!fileStatus.isDirectory()) {
            // found a file at depth which is less than number of partition keys
            logOrThrowExceptionWithMsg(
                "MSCK finds a file rather than a directory when it searches for "
                    + fileStatus.getPath().toString());
          } else {
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

  @VisibleForTesting
  void checkPartitionDirs(final ExecutorService executor,
      final Path basePath, final Set<Path> result,
      final FileSystem fs, final List<String> partColNames) throws MetastoreException {
    try {
      Queue<Future<Path>> futures = new LinkedList<>();
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
      LOG.error("Exception received while listing partition directories", e);
      executor.shutdownNow();
      throw new MetastoreException(e.getCause());
    }
  }
}
