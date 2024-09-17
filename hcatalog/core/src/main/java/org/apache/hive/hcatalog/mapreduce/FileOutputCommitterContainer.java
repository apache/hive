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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.har.HarOutputCommitterPostProcessor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part of the FileOutput*Container classes
 * See {@link FileOutputFormatContainer} for more information
 */
class FileOutputCommitterContainer extends OutputCommitterContainer {

  private static final String TEMP_DIR_NAME = "_temporary";
  private static final String LOGS_DIR_NAME = "_logs";

  static final String DYNTEMP_DIR_NAME = "_DYN";
  static final String SCRATCH_DIR_NAME = "_SCRATCH";
  private static final String APPEND_SUFFIX = "_a_";
  private static final int APPEND_COUNTER_WARN_THRESHOLD = 1000;
  private final int maxAppendAttempts;

  private static final Logger LOG = LoggerFactory.getLogger(FileOutputCommitterContainer.class);
  private final boolean dynamicPartitioningUsed;
  private boolean partitionsDiscovered;
  private final boolean customDynamicLocationUsed;

  private Map<String, Map<String, String>> partitionsDiscoveredByPath;
  private Map<String, JobContext> contextDiscoveredByPath;
  private final HiveStorageHandler cachedStorageHandler;

  HarOutputCommitterPostProcessor harProcessor = new HarOutputCommitterPostProcessor();

  private String ptnRootLocation = null;

  private OutputJobInfo jobInfo = null;

  /**
   * @param context current JobContext
   * @param baseCommitter OutputCommitter to contain
   * @throws IOException
   */
  public FileOutputCommitterContainer(JobContext context,
                    org.apache.hadoop.mapred.OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
    jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());
    dynamicPartitioningUsed = jobInfo.isDynamicPartitioningUsed();

    this.partitionsDiscovered = !dynamicPartitioningUsed;
    cachedStorageHandler = HCatUtil.getStorageHandler(context.getConfiguration(), jobInfo.getTableInfo().getStorerInfo());
    Table table = new Table(jobInfo.getTableInfo().getTable());
    if (dynamicPartitioningUsed && Boolean.parseBoolean((String)table.getProperty("EXTERNAL"))
        && jobInfo.getCustomDynamicPath() != null
        && jobInfo.getCustomDynamicPath().length() > 0) {
      customDynamicLocationUsed = true;
    } else {
      customDynamicLocationUsed = false;
    }

    this.maxAppendAttempts = context.getConfiguration().getInt(HCatConstants.HCAT_APPEND_LIMIT, APPEND_COUNTER_WARN_THRESHOLD);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().abortTask(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().abortTask(context);
      }
      finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
         //See HCATALOG-499
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().commitTask(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().commitTask(context);
      }
      finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      FileOutputFormatContainer.setWorkOutputPath(context);
      return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      // called explicitly through FileRecordWriterContainer.close() if dynamic - return false by default
      return true;
    }
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupJob(HCatMapRedUtil.createJobContext(context));
    }
    // in dynamic usecase, called through FileRecordWriterContainer
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupTask(HCatMapRedUtil.createTaskAttemptContext(context));
    }
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
    try {
      if (dynamicPartitioningUsed) {
        discoverPartitions(jobContext);
      }
      org.apache.hadoop.mapred.JobContext mapRedJobContext = HCatMapRedUtil
          .createJobContext(jobContext);
      if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
        getBaseOutputCommitter().abortJob(mapRedJobContext, state);
      } else if (dynamicPartitioningUsed) {
        for (JobContext currContext : contextDiscoveredByPath.values()) {
          try {
            new JobConf(currContext.getConfiguration())
                .getOutputCommitter().abortJob(currContext,
                    state);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      }
      Path src;
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext.getConfiguration());
      Path tblPath = new Path(jobInfo.getTableInfo().getTableLocation());
      if (dynamicPartitioningUsed) {
        if (!customDynamicLocationUsed) {
          src = new Path(getPartitionRootLocation(jobInfo.getLocation(), jobInfo.getTableInfo().getTable()
              .getPartitionKeysSize()));
        } else {
          src = new Path(getCustomPartitionRootLocation(jobInfo, jobContext.getConfiguration()));
        }
      } else {
        src = new Path(jobInfo.getLocation());
      }
      FileSystem fs = src.getFileSystem(jobContext.getConfiguration());
      // Note fs.delete will fail on Windows. The reason is in OutputCommitter,
      // Hadoop is still writing to _logs/history. On Linux, OS don't care file is still
      // open and remove the directory anyway, but on Windows, OS refuse to remove a
      // directory containing open files. So on Windows, we will leave output directory
      // behind when job fail. User needs to remove the output directory manually
      LOG.info("Job failed. Try cleaning up temporary directory [{}].", src);
      if (!src.equals(tblPath)){
        fs.delete(src, true);
      }
    } finally {
      cancelDelegationTokens(jobContext);
    }
  }

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      boolean filtered = name.equals(TEMP_DIR_NAME) || name.equals(LOGS_DIR_NAME) || name.equals(SUCCEEDED_FILE_NAME);
      return !filtered;
    }
  };

  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  private static boolean getOutputDirMarking(Configuration conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
      false);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    if (dynamicPartitioningUsed) {
      discoverPartitions(jobContext);
      // Commit each partition so it gets moved out of the job work
      // dir
      for (JobContext context : contextDiscoveredByPath.values()) {
        new JobConf(context.getConfiguration())
            .getOutputCommitter().commitJob(context);
      }
    }
    if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
      getBaseOutputCommitter().commitJob(
          HCatMapRedUtil.createJobContext(jobContext));
    }
    registerPartitions(jobContext);
    // create _SUCCESS FILE if so requested.
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext.getConfiguration());
    if (getOutputDirMarking(jobContext.getConfiguration())) {
      Path outputPath = new Path(jobInfo.getLocation());
      FileSystem fileSys = outputPath.getFileSystem(jobContext
          .getConfiguration());
      // create a file in the folder to mark it
      if (fileSys.exists(outputPath)) {
        Path filePath = new Path(outputPath,
            SUCCEEDED_FILE_NAME);
        if (!fileSys.exists(filePath)) { // may have been
                         // created by
                         // baseCommitter.commitJob()
          fileSys.create(filePath).close();
        }
      }
    }

    // Commit has succeeded (since no exceptions have been thrown.)
    // Safe to cancel delegation tokens now.
    cancelDelegationTokens(jobContext);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    throw new IOException("The method cleanupJob is deprecated and should not be called.");
  }

  private String getCustomPartitionRootLocation(OutputJobInfo jobInfo, Configuration conf) {
    if (ptnRootLocation == null) {
      // we only need to calculate it once, it'll be the same for other partitions in this job.
      String parentPath = jobInfo.getTableInfo().getTableLocation();
      if (jobInfo.getCustomDynamicRoot() != null
          && jobInfo.getCustomDynamicRoot().length() > 0) {
        parentPath = new Path(parentPath, jobInfo.getCustomDynamicRoot()).toString();
      }
      Path ptnRoot = new Path(parentPath, DYNTEMP_DIR_NAME +
          conf.get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID));
      ptnRootLocation = ptnRoot.toString();
    }
    return ptnRootLocation;
  }

  private String getPartitionRootLocation(String ptnLocn, int numPtnKeys) {
    if (customDynamicLocationUsed) {
      return null;
    }

    if (ptnRootLocation == null) {
      // we only need to calculate it once, it'll be the same for other partitions in this job.
      Path ptnRoot = new Path(ptnLocn);
      for (int i = 0; i < numPtnKeys; i++) {
//          LOG.info("Getting parent of "+ptnRoot.getName());
        ptnRoot = ptnRoot.getParent();
      }
      ptnRootLocation = ptnRoot.toString();
    }
//      LOG.info("Returning final parent : "+ptnRootLocation);
    return ptnRootLocation;
  }

  /**
   * Generate partition metadata object to be used to add to metadata.
   * @param context The job context.
   * @param jobInfo The OutputJobInfo.
   * @param partLocnRoot The table-equivalent location root of the partition
   *                       (temporary dir if dynamic partition, table dir if static)
   * @param dynPartPath The path of dynamic partition which is created
   * @param partKVs The keyvalue pairs that form the partition
   * @param outputSchema The output schema for the partition
   * @param params The parameters to store inside the partition
   * @param table The Table metadata object under which this Partition will reside
   * @param fs FileSystem object to operate on the underlying filesystem
   * @param conf HiveConf used to access FS
   * @param status Permission that's the default permission of the table dir.
   * @return Constructed Partition metadata object
   * @throws java.io.IOException
   */

  private Partition constructPartition(
      JobContext context, OutputJobInfo jobInfo,
      String partLocnRoot, String dynPartPath, Map<String, String> partKVs,
      HCatSchema outputSchema, Map<String, String> params,
      Table table, FileSystem fs, HiveConf conf,
      HdfsUtils.HadoopFileStatus status) throws IOException {

    Partition partition = new Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setSd(new StorageDescriptor(table.getTTable().getSd()));

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (HCatFieldSchema fieldSchema : outputSchema.getFields()) {
      fields.add(HCatSchemaUtils.getFieldSchema(fieldSchema));
    }

    partition.getSd().setCols(fields);

    partition.setValues(FileOutputFormatContainer.getPartitionValueList(table, partKVs));

    partition.setParameters(params);

    // Sets permissions and group name on partition dirs and files.

    Path partPath;
    if (customDynamicLocationUsed) {
      partPath = new Path(dynPartPath);
    } else if (!dynamicPartitioningUsed
         && Boolean.parseBoolean((String)table.getProperty("EXTERNAL"))
         && jobInfo.getLocation() != null && jobInfo.getLocation().length() > 0) {
      // Now, we need to de-scratchify this location - i.e., get rid of any
      // _SCRATCH[\d].?[\d]+ from the location.
      String jobLocation = jobInfo.getLocation();
      String finalLocn = jobLocation.replaceAll(Path.SEPARATOR + SCRATCH_DIR_NAME + "\\d\\.?\\d+","");
      partPath = new Path(finalLocn);
    } else {
      partPath = new Path(partLocnRoot);
      FileSystem partFs = partPath.getFileSystem(context.getConfiguration());
      int i = 0;
      for (FieldSchema partKey : table.getPartitionKeys()) {
        if (i++ != 0) {
          partFs.mkdirs(partPath); // Attempt to make the path in case it does not exist before we check
          HdfsUtils.setFullFileStatus(conf, status, status.getFileStatus().getGroup(), fs,
              partPath, false);
        }
        partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
      }
    }

    // Do not need to set the status on the partition directory. We will do it later recursively.
    // See: end of the registerPartitions method
    FileSystem partFs = partPath.getFileSystem(context.getConfiguration());
    partFs.mkdirs(partPath);

    // Set the location in the StorageDescriptor
    if (dynamicPartitioningUsed) {
      String dynamicPartitionDestination = getFinalDynamicPartitionDestination(table, partKVs, jobInfo);
      if (harProcessor.isEnabled()) {
        harProcessor.exec(context, partition, partPath);
        partition.getSd().setLocation(
          harProcessor.getProcessedLocation(new Path(dynamicPartitionDestination)));
      } else {
        partition.getSd().setLocation(dynamicPartitionDestination);
      }
    } else {
      partition.getSd().setLocation(partPath.toString());
    }
    return partition;
  }

  private String getFinalDynamicPartitionDestination(Table table, Map<String, String> partKVs,
      OutputJobInfo jobInfo) {
    Path partPath = new Path(table.getTTable().getSd().getLocation());
    if (!customDynamicLocationUsed) {
      // file:///tmp/hcat_junit_warehouse/employee/_DYN0.7770480401313761/emp_country=IN/emp_state=KA  ->
      // file:///tmp/hcat_junit_warehouse/employee/emp_country=IN/emp_state=KA
      for (FieldSchema partKey : table.getPartitionKeys()) {
        partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
      }

      return partPath.toString();
    } else {
      // if custom root specified, update the parent path
      if (jobInfo.getCustomDynamicRoot() != null
          && jobInfo.getCustomDynamicRoot().length() > 0) {
        partPath = new Path(partPath, jobInfo.getCustomDynamicRoot());
      }
      return new Path(partPath, HCatFileUtil.resolveCustomPath(jobInfo, partKVs, false)).toString();
    }
  }

  private Map<String, String> getStorerParameterMap(StorerInfo storer) {
    Map<String, String> params = new HashMap<String, String>();

    //Copy table level hcat.* keys to the partition
    for (Entry<Object, Object> entry : storer.getProperties().entrySet()) {
      if (!entry.getKey().toString().equals(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
        params.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }
    return params;
  }

  private Path constructPartialPartPath(Path partialPath, String partKey, Map<String, String> partKVs) {

    StringBuilder sb = new StringBuilder(FileUtils.escapePathName(partKey));
    sb.append("=");
    sb.append(FileUtils.escapePathName(partKVs.get(partKey)));
    return new Path(partialPath, sb.toString());
  }

  /**
   * Update table schema, adding new columns as added for the partition.
   * @param client the client
   * @param table the table
   * @param partitionSchema the schema of the partition
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   * @throws org.apache.hadoop.hive.metastore.api.InvalidOperationException the invalid operation exception
   * @throws org.apache.hadoop.hive.metastore.api.MetaException the meta exception
   * @throws org.apache.thrift.TException the t exception
   */
  private void updateTableSchema(IMetaStoreClient client, Table table,
                   HCatSchema partitionSchema) throws IOException, InvalidOperationException, MetaException, TException {


    List<FieldSchema> newColumns = HCatUtil.validatePartitionSchema(table, partitionSchema);

    if (newColumns.size() != 0) {
      List<FieldSchema> tableColumns = new ArrayList<FieldSchema>(table.getTTable().getSd().getCols());
      tableColumns.addAll(newColumns);

      //Update table schema to add the newly added columns
      table.getTTable().getSd().setCols(tableColumns);
      client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
    }
  }

  /**
   * Move task output from the temp directory to the final location
   * @param srcf the file to move
   * @param srcDir the source directory
   * @param destDir the target directory
   * @param immutable - whether table is immutable.
   * @throws java.io.IOException
   */
  private void moveTaskOutputs(final Configuration conf, Path srcf, Path srcDir,
      Path destDir, boolean immutable) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("moveTaskOutputs "
          + srcf + " from: " + srcDir + " to: " + destDir + " immutable: " + immutable);
    }

    if (dynamicPartitioningUsed) {
      immutable = true; // Making sure we treat dynamic partitioning jobs as if they were immutable.
    }

    final FileSystem srcFs = srcf.getFileSystem(conf);
    final FileSystem destFs = destDir.getFileSystem(conf);
    final boolean canRename = srcFs.getUri().equals(destFs.getUri());

    if (destFs.exists(destDir) && !destFs.getFileStatus(destDir).isDirectory()) {
      throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Destination is not directory " + destDir);
    }

    LinkedList<Pair<Path, Path>> moves = new LinkedList<>();
    if (customDynamicLocationUsed) {
      if (immutable && destFs.exists(destDir)
          && !org.apache.hadoop.hive.metastore.utils.FileUtils.isDirEmpty(destFs, destDir)) {
        throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION,
            "Data already exists in " + destDir
                + ", duplicate publish not possible.");
      }
      moves.add(Pair.of(srcf, destDir));
    } else {
      Queue<FileStatus> srcQ = new LinkedList<>();
      FileStatus[] contents = srcFs.listStatus(srcf, HIDDEN_FILES_PATH_FILTER);
      if (contents.length == 0) {
        // nothing to move
        return;
      }
      Collections.addAll(srcQ, contents);

      while (!srcQ.isEmpty()) {
        FileStatus srcStatus = srcQ.remove();
        Path srcF = srcStatus.getPath();
        final Path finalOutputPath = getFinalPath(destFs, srcF, srcDir, destDir, immutable);
        if (immutable && destFs.exists(finalOutputPath)
            && !org.apache.hadoop.hive.metastore.utils.FileUtils.isDirEmpty(destFs, finalOutputPath)) {
          if (partitionsDiscoveredByPath.containsKey(srcF.toString())) {
            throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION,
                "Data already exists in " + finalOutputPath + ", duplicate publish not possible.");
          }
          // parent directory may exist for multi-partitions, check lower level partitions
          Collections.addAll(srcQ, srcFs.listStatus(srcF, HIDDEN_FILES_PATH_FILTER));
        } else if (srcStatus.isDirectory()) {
          if (canRename && dynamicPartitioningUsed) {
            // If it is partition, move the partition directory instead of each file.
            moves.add(Pair.of(srcF, finalOutputPath));
          } else {
            Collections.addAll(srcQ, srcFs.listStatus(srcF, HIDDEN_FILES_PATH_FILTER));
          }
        } else {
          moves.add(Pair.of(srcF, finalOutputPath));
        }
      }
    }

    bulkMoveFiles(conf, srcFs, destFs, moves);
  }

  /**
   * Bulk move files from source to destination.
   * @param srcFs the source filesystem where the source files are
   * @param destFs the destionation filesystem where the destionation files are
   * @param pairs list of pairs of <source_path, destination_path>, move source_path to destination_path
   * @throws java.io.IOException
   */
  private void bulkMoveFiles(final Configuration conf, final FileSystem srcFs, final FileSystem destFs,
      final List<Pair<Path, Path>> pairs) throws IOException {
    if (pairs.isEmpty()) {
      return;
    }
    final boolean canRename = srcFs.getUri().equals(destFs.getUri());
    final List<Future<Pair<Path, Path>>> futures = new LinkedList<>();
    final int moveThreadsCount = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25);

    if (moveThreadsCount <= 0) {
      for (final Pair<Path, Path> pair: pairs) {
        Path srcP = pair.getLeft();
        Path dstP = pair.getRight();
        if (!moveFile(srcFs, srcP, destFs, dstP, conf, canRename)) {
          throw new HCatException(ErrorType.ERROR_MOVE_FAILED,
              "Unable to move from " + srcP + " to " + dstP);
        }
      }
      return;
    }

    final ExecutorService pool = Executors.newFixedThreadPool(moveThreadsCount,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build());

    for (final Pair<Path, Path> pair: pairs) {
      Path srcP = pair.getLeft();
      Path dstP = pair.getRight();
      futures.add(pool.submit(new Callable<Pair<Path, Path>>() {
        @Override
        public Pair<Path, Path> call() throws IOException {
          if (moveFile(srcFs, srcP, destFs, dstP, conf, canRename)) {
            return pair;
          } else {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED,
                "Unable to move from " + srcP + " to " + dstP);
          }
        }
      }));
    }
    pool.shutdown();
    for (Future<Pair<Path, Path>> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        pool.shutdownNow();
        throw new HCatException(ErrorType.ERROR_MOVE_FAILED, e.getMessage());
      }
    }
  }

  private boolean moveFile(final FileSystem srcFs, final Path srcf, final FileSystem destFs, final Path destf,
      final Configuration conf, final boolean canRename) throws IOException {
    LOG.debug("Moving src: {}, to dest: {}", srcf, destf);
    boolean moved;
    if (canRename) {
      destFs.mkdirs(destf.getParent());
      moved = srcFs.rename(srcf, destf);
    } else {
      moved = FileUtil.copy(srcFs, srcf, destFs, destf, true, false, conf);
    }
    return moved;
  }

  /**
   * Find the final name of a given output file, given the output directory
   * and the work directory. If immutable, attempt to create file of name
   * _aN till we find an item that does not exist.
   * @param file the file to move
   * @param src the source directory
   * @param dest the target directory
   * @return the final path for the specific output file
   * @throws java.io.IOException
   */
  private Path getFinalPath(FileSystem fs, Path file, Path src,
                Path dest, final boolean immutable) throws IOException {
    URI taskOutputUri = file.toUri();
    URI relativePath = src.toUri().relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {
      throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Can not get the relative path: base = " +
        src + " child = " + file);
    }
    if (relativePath.getPath().length() > 0) {

      Path itemDest = new Path(dest, relativePath.getPath());
      if (!immutable){
        String name = relativePath.getPath();
        String filetype;
        int index = name.lastIndexOf('.');
        if (index >= 0) {
          filetype = name.substring(index);
          name = name.substring(0, index);
        } else {
          filetype = "";
        }

        // Attempt to find maxAppendAttempts possible alternatives to a filename by
        // appending _a_N and seeing if that destination also clashes. If we're
        // still clashing after that, give up.
        int counter = 1;
        for (; fs.exists(itemDest) && counter < maxAppendAttempts; counter++) {
          itemDest = new Path(dest, name + (APPEND_SUFFIX + counter) + filetype);
        }

        if (counter == maxAppendAttempts){
          throw new HCatException(ErrorType.ERROR_MOVE_FAILED,
              "Could not find a unique destination path for move: file = "
                  + file + " , src = " + src + ", dest = " + dest);
        } else if (counter > APPEND_COUNTER_WARN_THRESHOLD) {
          LOG.warn("Append job used filename clash counter [" + counter
              +"] which is greater than warning limit [" + APPEND_COUNTER_WARN_THRESHOLD
              +"]. Please compact this table so that performance is not impacted."
              + " Please see HIVE-9381 for details.");
        }

      }

      if (LOG.isDebugEnabled()){
        LOG.debug("FinalPath(file:"+file+":"+src+"->"+dest+"="+itemDest);
      }

      return itemDest;
    } else {

      return dest;
    }
  }

  /**
   * Run to discover dynamic partitions available
   */
  private void discoverPartitions(JobContext context) throws IOException {
    if (!partitionsDiscovered) {
      //      LOG.info("discover ptns called");
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());

      harProcessor.setEnabled(jobInfo.getHarRequested());

      List<Integer> dynamicPartCols = jobInfo.getPosOfDynPartCols();
      int maxDynamicPartitions = jobInfo.getMaxDynamicPartitions();

      Path loadPath = new Path(jobInfo.getLocation());
      FileSystem fs = loadPath.getFileSystem(context.getConfiguration());

      // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
      String dynPathSpec = loadPath.toUri().getPath();
      dynPathSpec = dynPathSpec.replace("__HIVE_DEFAULT_PARTITION__", "*");

      //      LOG.info("Searching for "+dynPathSpec);
      Path pathPattern = new Path(dynPathSpec);
      FileStatus[] status = fs.globStatus(pathPattern, FileUtils.HIDDEN_FILES_PATH_FILTER);

      partitionsDiscoveredByPath = new LinkedHashMap<String, Map<String, String>>();
      contextDiscoveredByPath = new LinkedHashMap<String, JobContext>();


      if (status.length == 0) {
        //        LOG.warn("No partition found genereated by dynamic partitioning in ["
        //            +loadPath+"] with depth["+jobInfo.getTable().getPartitionKeysSize()
        //            +"], dynSpec["+dynPathSpec+"]");
      } else {
        if ((maxDynamicPartitions != -1) && (status.length > maxDynamicPartitions)) {
          this.partitionsDiscovered = true;
          throw new HCatException(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS,
            "Number of dynamic partitions being created "
              + "exceeds configured max allowable partitions["
              + maxDynamicPartitions
              + "], increase parameter ["
              + HiveConf.ConfVars.DYNAMIC_PARTITION_MAX_PARTS.varname
              + "] if needed.");
        }

        for (FileStatus st : status) {
          LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>();
          if (!customDynamicLocationUsed) {
            Warehouse.makeSpecFromName(fullPartSpec, st.getPath(), null);
          } else {
            HCatFileUtil.getPartKeyValuesForCustomLocation(fullPartSpec, jobInfo,
                st.getPath().toString());
          }
          partitionsDiscoveredByPath.put(st.getPath().toString(), fullPartSpec);
          JobConf jobConf = (JobConf)context.getConfiguration();
          JobContext currContext = HCatMapRedUtil.createJobContext(
            jobConf,
            context.getJobID(),
            InternalUtil.createReporter(HCatMapRedUtil.createTaskAttemptContext(jobConf,
              ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID())));
          HCatOutputFormat.configureOutputStorageHandler(currContext, jobInfo, fullPartSpec);
          contextDiscoveredByPath.put(st.getPath().toString(), currContext);
        }
      }

      //      for (Entry<String,Map<String,String>> spec : partitionsDiscoveredByPath.entrySet()){
      //        LOG.info("Partition "+ spec.getKey());
      //        for (Entry<String,String> e : spec.getValue().entrySet()){
      //          LOG.info(e.getKey() + "=>" +e.getValue());
      //        }
      //      }

      this.partitionsDiscovered = true;
    }
  }

  private void registerPartitions(JobContext context) throws IOException{
    if (dynamicPartitioningUsed){
      discoverPartitions(context);
    }
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());
    Configuration conf = context.getConfiguration();
    Table table = new Table(jobInfo.getTableInfo().getTable());
    Path tblPath = new Path(table.getTTable().getSd().getLocation());
    FileSystem fs = tblPath.getFileSystem(conf);
    IMetaStoreClient client = null;
    HCatTableInfo tableInfo = jobInfo.getTableInfo();
    List<Partition> partitionsAdded = new ArrayList<Partition>();
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      if (table.getPartitionKeys().size() == 0) {
        // Move data from temp directory the actual table directory
        // No metastore operation required.
        Path src = new Path(jobInfo.getLocation());
        moveTaskOutputs(conf, src, src, tblPath, table.isImmutable());
        if (!src.equals(tblPath)) {
          fs.delete(src, true);
        }
        if (table.getParameters() != null
            && table.getParameters().containsKey(StatsSetupConst.COLUMN_STATS_ACCURATE)) {
          table.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
          client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
        }
        return;
      }

      StorerInfo storer = InternalUtil.extractStorerInfo(table.getTTable().getSd(),
          table.getParameters());

      HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(conf, fs, tblPath);

      List<Partition> partitionsToAdd = new ArrayList<Partition>();
      if (!dynamicPartitioningUsed) {
        partitionsToAdd.add(constructPartition(context, jobInfo, tblPath.toString(), null,
            jobInfo.getPartitionValues(), jobInfo.getOutputSchema(), getStorerParameterMap(storer),
            table, fs, hiveConf, status));
      } else {
        for (Entry<String, Map<String, String>> entry : partitionsDiscoveredByPath.entrySet()) {
          partitionsToAdd.add(constructPartition(context, jobInfo,
              getPartitionRootLocation(entry.getKey(), entry.getValue().size()), entry.getKey(),
              entry.getValue(), jobInfo.getOutputSchema(), getStorerParameterMap(storer), table,
              fs, hiveConf, status));
        }
      }

      ArrayList<Map<String,String>> ptnInfos = new ArrayList<Map<String,String>>();
      for(Partition ptn : partitionsToAdd){
        ptnInfos.add(InternalUtil.createPtnKeyValueMap(new Table(tableInfo.getTable()), ptn));
      }

      /**
       * Dynamic partitioning & Append incompatibility note:
       *
       * Currently, we do not support mixing dynamic partitioning and append in the
       * same job. One reason is that we need exhaustive testing of corner cases
       * for that, and a second reason is the behaviour of add_partitions. To support
       * dynamic partitioning with append, we'd have to have a add_partitions_if_not_exist
       * call, rather than an add_partitions call. Thus far, we've tried to keep the
       * implementation of append jobtype-agnostic, but here, in code, we assume that
       * a table is considered immutable if dynamic partitioning is enabled on the job.
       *
       * This does not mean that we can check before the job begins that this is going
       * to be a dynamic partition job on an immutable table and thus fail the job, since
       * it is quite possible to have a dynamic partitioning job run on an unpopulated
       * immutable table. It simply means that at the end of the job, as far as copying
       * in data is concerned, we will pretend that the table is immutable irrespective
       * of what table.isImmutable() tells us.
       */

      //Publish the new partition(s)
      if (dynamicPartitioningUsed && harProcessor.isEnabled() && (!partitionsToAdd.isEmpty())){

        if (!customDynamicLocationUsed) {
          Path src = new Path(ptnRootLocation);
          // check here for each dir we're copying out, to see if it
          // already exists, error out if so.
          // Also, treat dyn-writes as writes to immutable tables.
          moveTaskOutputs(conf, src, src, tblPath, table.isImmutable());
          if (!src.equals(tblPath)){
            fs.delete(src, true);
          }
        } else {
          moveCustomLocationTaskOutputs(fs, table, hiveConf);
        }
        try {
          updateTableSchema(client, table, jobInfo.getOutputSchema());
          LOG.info("HAR is being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
          client.add_partitions(partitionsToAdd);
          partitionsAdded = partitionsToAdd;
        } catch (Exception e){
          // There was an error adding partitions : rollback fs copy and rethrow
          for (Partition p : partitionsToAdd){
            Path ptnPath = new Path(harProcessor.getParentFSPath(new Path(p.getSd().getLocation())));
            if (fs.exists(ptnPath)){
              fs.delete(ptnPath,true);
            }
          }
          throw e;
        }

      }else{

        // no harProcessor, regular operation
        updateTableSchema(client, table, jobInfo.getOutputSchema());
        LOG.info("HAR not is not being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
        if (partitionsToAdd.size() > 0){
          if (!dynamicPartitioningUsed ) {

            // regular single-partition write into a partitioned table.
            //Move data from temp directory the actual table directory
            if (partitionsToAdd.size() > 1){
              throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION,
                  "More than one partition to publish in non-dynamic partitioning job");
            }
            Partition p = partitionsToAdd.get(0);
            Path src = new Path(jobInfo.getLocation());
            Path dest = new Path(p.getSd().getLocation());
            moveTaskOutputs(conf, src, src, dest, table.isImmutable());
            if (!src.equals(dest)){
              if (src.toString().matches(".*" + Path.SEPARATOR + SCRATCH_DIR_NAME + "\\d\\.?\\d+.*")){
                // src is scratch directory, need to trim the part key value pairs from path
                String diff = StringUtils.difference(src.toString(), dest.toString());
                fs.delete(new Path(StringUtils.substringBefore(src.toString(), diff)), true);
              } else {
                fs.delete(src, true);
              }
            }

            // Now, we check if the partition already exists. If not, we go ahead.
            // If so, we error out if immutable, and if mutable, check that the partition's IF
            // matches our current job's IF (table's IF) to check for compatibility. If compatible, we
            // ignore and do not add. If incompatible, we error out again.

            boolean publishRequired = false;
            try {
              Partition existingP = client.getPartition(p.getDbName(),p.getTableName(),p.getValues());
              if (existingP != null){
                if (table.isImmutable()){
                  throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION,
                      "Attempted duplicate partition publish on to immutable table");
                } else {
                  if (! existingP.getSd().getInputFormat().equals(table.getInputFormatClass().getName())){
                    throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION,
                        "Attempted partition append, where old partition format was "
                            + existingP.getSd().getInputFormat()
                            + " and table format was "
                            + table.getInputFormatClass().getName());
                  }
                }
              } else {
                publishRequired = true;
              }
            } catch (NoSuchObjectException e){
              // All good, no such partition exists, move on.
              publishRequired = true;
            }
            if (publishRequired){
              client.add_partitions(partitionsToAdd);
              partitionsAdded = partitionsToAdd;
            }

          } else {
            // Dynamic partitioning usecase
            if (!customDynamicLocationUsed) {
              Path src = new Path(ptnRootLocation);
              moveTaskOutputs(conf, src, src, tblPath, true);
              if (!src.equals(tblPath)){
                fs.delete(src, true);
              }
            } else {
              moveCustomLocationTaskOutputs(fs, table, hiveConf);
            }
            client.add_partitions(partitionsToAdd);
            partitionsAdded = partitionsToAdd;
          }
        }

        // Set permissions appropriately for each of the partitions we just created
        // so as to have their permissions mimic the table permissions
        for (Partition p : partitionsAdded){
          HdfsUtils.setFullFileStatus(conf, status, status.getFileStatus().getGroup(), fs,
              new Path(p.getSd().getLocation()), true);
        }
      }
    } catch (Exception e) {
      if (partitionsAdded.size() > 0) {
        try {
          // baseCommitter.cleanupJob failed, try to clean up the
          // metastore
          for (Partition p : partitionsAdded) {
            client.dropPartition(tableInfo.getDatabaseName(),
                tableInfo.getTableName(), p.getValues(), true);
          }
        } catch (Exception te) {
          // Keep cause as the original exception
          throw new HCatException(
              ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      }
      if (e instanceof HCatException) {
        throw (HCatException) e;
      } else {
        throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  private void moveCustomLocationTaskOutputs(FileSystem fs, Table table, Configuration conf)
    throws IOException {
    // in case of custom dynamic partitions, we can't just move the sub-tree of partition root
    // directory since the partitions location contain regex pattern. We need to first find the
    // final destination of each partition and move its output.
    for (Entry<String, Map<String, String>> entry : partitionsDiscoveredByPath.entrySet()) {
      Path src = new Path(entry.getKey());
      Path destPath = new Path(getFinalDynamicPartitionDestination(table, entry.getValue(), jobInfo));
      moveTaskOutputs(conf, src, src, destPath, true);
    }
    // delete the parent temp directory of all custom dynamic partitions
    Path parentPath = new Path(getCustomPartitionRootLocation(jobInfo, conf));
    if (fs.exists(parentPath)) {
      fs.delete(parentPath, true);
    }
  }

  private void cancelDelegationTokens(JobContext context) throws IOException{
    LOG.info("Cancelling delegation token for the job.");
    IMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatUtil
          .getHiveConf(context.getConfiguration());
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      // cancel the deleg. tokens that were acquired for this job now that
      // we are done - we should cancel if the tokens were acquired by
      // HCatOutputFormat and not if they were supplied by Oozie.
      // In the latter case the HCAT_KEY_TOKEN_SIGNATURE property in
      // the conf will not be set
      String tokenStrForm = client.getTokenStrForm();
      String hCatKeyTokenSignature = context.getConfiguration().get(
          HCatConstants.HCAT_KEY_TOKEN_SIGNATURE);
      if (tokenStrForm != null
          && hCatKeyTokenSignature != null) {
        LOG.info("FileOutputCommitterContainer::cancelDelegationTokens(): " +
            "Cancelling token fetched for HCAT_KEY_TOKEN_SIGNATURE == (" + hCatKeyTokenSignature + ").");
        client.cancelDelegationToken(tokenStrForm);
      }
      else {
        LOG.info("FileOutputCommitterContainer::cancelDelegationTokens(): " +
            "Could not find tokenStrForm, or HCAT_KEY_TOKEN_SIGNATURE. Skipping token cancellation.");
      }
    } catch (MetaException e) {
      LOG.warn("MetaException while cancelling delegation token.", e);
    } catch (TException e) {
      LOG.warn("TException while cancelling delegation token.", e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }


}
