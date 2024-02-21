/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.writer.HiveIcebergWriter;
import org.apache.iceberg.mr.hive.writer.WriterRegistry;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Iceberg table committer for adding data files to the Iceberg tables.
 * Currently independent of the Hive ACID transactions.
 */
public class HiveIcebergOutputCommitter extends OutputCommitter {
  private static final String FOR_COMMIT_EXTENSION = ".forCommit";

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) {
    // do nothing.
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) {
    // do nothing.
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) {
    // We need to commit if this is the last phase of a MapReduce process
    return TaskType.REDUCE.equals(context.getTaskAttemptID().getTaskID().getTaskType()) ||
        context.getJobConf().getNumReduceTasks() == 0;
  }

  /**
   * Collects the generated data files and creates a commit file storing the data file list.
   * @param originalContext The task attempt context
   * @throws IOException Thrown if there is an error writing the commit file
   */
  @Override
  public void commitTask(TaskAttemptContext originalContext) throws IOException {
    TaskAttemptContext context = TezUtil.enrichContextWithAttemptWrapper(originalContext);

    TaskAttemptID attemptID = context.getTaskAttemptID();
    JobConf jobConf = context.getJobConf();
    Set<String> outputs = HiveIcebergStorageHandler.outputTables(context.getJobConf());
    Map<String, List<HiveIcebergWriter>> writers = Optional.ofNullable(WriterRegistry.writers(attemptID))
        .orElseGet(() -> {
          LOG.info("CommitTask found no writers for output tables: {}, attemptID: {}", outputs, attemptID);
          return ImmutableMap.of();
        });

    ExecutorService tableExecutor = tableExecutor(jobConf, outputs.size());
    try {
      // Generates commit files for the target tables in parallel
      Tasks.foreach(outputs)
          .retry(3)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(tableExecutor)
          .run(output -> {
            Table table = HiveIcebergStorageHandler.table(context.getJobConf(), output);
            if (table != null) {
              String fileForCommitLocation = generateFileForCommitLocation(table.location(), jobConf,
                  attemptID.getJobID(), attemptID.getTaskID().getId());
              if (writers.get(output) != null) {
                Collection<DataFile> dataFiles = Lists.newArrayList();
                Collection<DeleteFile> deleteFiles = Lists.newArrayList();
                Collection<DataFile> referencedDataFiles = Lists.newArrayList();
                for (HiveIcebergWriter writer : writers.get(output)) {
                  FilesForCommit files = writer.files();
                  dataFiles.addAll(files.dataFiles());
                  deleteFiles.addAll(files.deleteFiles());
                  referencedDataFiles.addAll(files.referencedDataFiles());
                }
                createFileForCommit(new FilesForCommit(dataFiles, deleteFiles, referencedDataFiles),
                    fileForCommitLocation, table.io());
              } else {
                LOG.info("CommitTask found no writer for specific table: {}, attemptID: {}", output, attemptID);
                createFileForCommit(FilesForCommit.empty(), fileForCommitLocation, table.io());
              }
            } else {
              // When using Tez multi-table inserts, we could have more output tables in config than
              // the actual tables this task has written to and has serialized in its config
              LOG.info("CommitTask found no serialized table in config for table: {}.", output);
            }
          }, IOException.class);
    } finally {
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    // remove the writer to release the object
    WriterRegistry.removeWriters(attemptID);
  }

  /**
   * Removes files generated by this task.
   * @param originalContext The task attempt context
   * @throws IOException Thrown if there is an error closing the writer
   */
  @Override
  public void abortTask(TaskAttemptContext originalContext) throws IOException {
    TaskAttemptContext context = TezUtil.enrichContextWithAttemptWrapper(originalContext);

    // Clean up writer data from the local store
    Map<String, List<HiveIcebergWriter>> writerMap = WriterRegistry.removeWriters(context.getTaskAttemptID());

    // Remove files if it was not done already
    if (writerMap != null) {
      for (List<HiveIcebergWriter> writerList : writerMap.values()) {
        for (HiveIcebergWriter writer : writerList) {
          writer.close(true);
        }
      }
    }
  }

  @Override
  public void commitJob(JobContext originalContext) throws IOException {
    commitJobs(Collections.singletonList(originalContext), Operation.OTHER);
  }

  /**
   * Wrapper class for storing output {@link Table} and it's context for committing changes:
   * JobContext, CommitInfo.
   */
  private static class OutputTable {
    private final String catalogName;
    private final String tableName;
    private final Table table;
    private List<JobContext> jobContexts;

    private OutputTable(String catalogName, String tableName, Table table) {
      this.catalogName = catalogName;
      this.tableName = tableName;
      this.table = table;
    }

    public void setJobContexts(List<JobContext> jobContexts) {
      this.jobContexts = ImmutableList.copyOf(jobContexts);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OutputTable output1 = (OutputTable) o;
      return Objects.equals(tableName, output1.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName);
    }
  }

  /**
   * Reads the commit files stored in the temp directories and collects the generated committed data files.
   * Appends the data files to the tables. At the end removes the temporary directories.
   * @param originalContextList The job context list
   * @throws IOException if there is a failure accessing the files
   */
  public void commitJobs(List<JobContext> originalContextList, Operation operation) throws IOException {
    List<JobContext> jobContextList = originalContextList.stream()
        .map(TezUtil::enrichContextWithVertexId)
        .collect(Collectors.toList());
    List<OutputTable> outputs = collectOutputs(jobContextList);
    long startTime = System.currentTimeMillis();

    String ids = jobContextList.stream()
        .map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(","));
    LOG.info("Committing job(s) {} has started", ids);

    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();
    ExecutorService fileExecutor = fileExecutor(jobContextList.get(0).getJobConf());
    ExecutorService tableExecutor = tableExecutor(jobContextList.get(0).getJobConf(), outputs.size());
    try {
      // Commits the changes for the output tables in parallel
      Tasks.foreach(outputs)
          .throwFailureWhenFinished()
          .stopOnFailure()
          .executeWith(tableExecutor)
          .run(output -> {
            Table table = output.table;
            jobLocations.addAll(
                output.jobContexts.stream().map(jobContext ->
                  generateJobLocation(table.location(), jobContext.getJobConf(), jobContext.getJobID()))
                .collect(Collectors.toList()));
            commitTable(table.io(), fileExecutor, output, operation);
          });
    } finally {
      fileExecutor.shutdown();
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    LOG.info("Commit took {} ms for job(s) {}", System.currentTimeMillis() - startTime, ids);

    for (JobContext jobContext : jobContextList) {
      cleanup(jobContext, jobLocations);
    }
  }

  private List<OutputTable> collectOutputs(List<JobContext> jobContextList) {
    return jobContextList.stream()
      .flatMap(jobContext -> HiveIcebergStorageHandler.outputTables(jobContext.getJobConf()).stream()
        .map(output -> new OutputTable(
          HiveIcebergStorageHandler.catalogName(jobContext.getJobConf(), output),
          output,
          SessionStateUtil.getResource(jobContext.getJobConf(), output)
            .filter(o -> o instanceof Table).map(o -> (Table) o)
            // fall back to getting the serialized table from the config
            .orElseGet(() -> HiveIcebergStorageHandler.table(jobContext.getJobConf(), output))))
        .filter(output -> Objects.nonNull(output.table))
        .map(output -> new SimpleImmutableEntry<>(output, jobContext)))
      .collect(
        Collectors.groupingBy(Map.Entry::getKey,
          Collectors.mapping(Map.Entry::getValue, Collectors.toList()))
      ).entrySet().stream().map(
        kv -> {
          kv.getKey().setJobContexts(kv.getValue());
          return kv.getKey();
        })
      .collect(Collectors.toList());
  }

  /**
   * Removes the generated data files if there is a commit file already generated for them.
   * The cleanup at the end removes the temporary directories as well.
   * @param originalContext The job context
   * @param status The status of the job
   * @throws IOException if there is a failure deleting the files
   */
  @Override
  public void abortJob(JobContext originalContext, int status) throws IOException {
    abortJobs(Collections.singletonList(originalContext));
  }

  public void abortJobs(List<JobContext> originalContextList) throws IOException {
    List<JobContext> jobContextList = originalContextList.stream()
        .map(TezUtil::enrichContextWithVertexId)
        .collect(Collectors.toList());
    List<OutputTable> outputs = collectOutputs(jobContextList);

    String ids = jobContextList.stream()
        .map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(","));
    LOG.info("Job(s) {} are aborted. Data file cleaning started", ids);

    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();

    ExecutorService fileExecutor = fileExecutor(jobContextList.get(0).getJobConf());
    ExecutorService tableExecutor = tableExecutor(jobContextList.get(0).getJobConf(), outputs.size());
    try {
      // Cleans up the changes for the output tables in parallel
      Tasks.foreach(outputs.stream().flatMap(kv -> kv.jobContexts.stream()
              .map(jobContext -> new SimpleImmutableEntry<>(kv.table, jobContext))))
          .suppressFailureWhenFinished()
          .executeWith(tableExecutor)
          .onFailure((output, exc) -> LOG.warn("Failed cleanup table {} on abort job", output, exc))
          .run(output -> {
            JobContext jobContext = output.getValue();
            JobConf jobConf = jobContext.getJobConf();
            LOG.info("Cleaning job for jobID: {}, table: {}", jobContext.getJobID(), output);

            Table table = output.getKey();
            String jobLocation = generateJobLocation(table.location(), jobConf, jobContext.getJobID());
            jobLocations.add(jobLocation);
            // list jobLocation to get number of forCommit files
            // we do this because map/reduce num in jobConf is unreliable and we have no access to vertex status info
            int numTasks = listForCommits(jobConf, jobLocation).size();
            FilesForCommit results = collectResults(numTasks, fileExecutor, table.location(), jobContext,
                table.io(), false);
            // Check if we have files already written and remove data and delta files if there are any
            Tasks.foreach(results.allFiles())
                .retry(3)
                .suppressFailureWhenFinished()
                .executeWith(fileExecutor)
                .onFailure((file, exc) -> LOG.warn("Failed to remove data file {} on abort job", file.path(), exc))
                .run(file -> table.io().deleteFile(file.path().toString()));
          }, IOException.class);
    } finally {
      fileExecutor.shutdown();
      if (tableExecutor != null) {
        tableExecutor.shutdown();
      }
    }

    LOG.info("Job(s) {} are aborted. Data file cleaning finished", ids);

    for (JobContext jobContext : jobContextList) {
      cleanup(jobContext, jobLocations);
    }
  }

  /**
   * Lists the forCommit files under a job location. This should only be used by {@link #abortJob(JobContext, int)},
   * since on the Tez AM-side it will have no access to the correct number of writer tasks otherwise. The commitJob
   * should not need to use this listing as it should have access to the vertex status info on the HS2-side.
   * @param jobConf jobConf used for getting the FS
   * @param jobLocation The job location that we should list
   * @return The set of forCommit files under the job location
   * @throws IOException if the listing fails
   */
  private Set<FileStatus> listForCommits(JobConf jobConf, String jobLocation) throws IOException {
    Path path = new Path(jobLocation);
    LOG.debug("Listing job location to get forCommits for abort: {}", jobLocation);
    FileStatus[] children = path.getFileSystem(jobConf).listStatus(path);
    LOG.debug("Listing the job location: {} yielded these files: {}", jobLocation, Arrays.toString(children));
    return Arrays.stream(children)
        .filter(child -> !child.isDirectory() && child.getPath().getName().endsWith(FOR_COMMIT_EXTENSION))
        .collect(Collectors.toSet());
  }

  /**
   * Collects the additions to a single table and adds/commits the new files to the Iceberg table.
   * @param io The io to read the forCommit files
   * @param executor The executor used to read the forCommit files
   * @param outputTable The table used for loading from the catalog
   */
  private void commitTable(FileIO io, ExecutorService executor, OutputTable outputTable, Operation operation) {
    String name = outputTable.tableName;
    Properties catalogProperties = new Properties();
    catalogProperties.put(Catalogs.NAME, name);
    catalogProperties.put(Catalogs.LOCATION, outputTable.table.location());
    if (outputTable.catalogName != null) {
      catalogProperties.put(InputFormatConfig.CATALOG_NAME, outputTable.catalogName);
    }
    List<DataFile> dataFiles = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    List<DataFile> referencedDataFiles = Lists.newArrayList();

    Table table = null;
    String branchName = null;

    for (JobContext jobContext : outputTable.jobContexts) {
      JobConf conf = jobContext.getJobConf();
      table = Optional.ofNullable(table).orElse(Catalogs.loadTable(conf, catalogProperties));
      branchName = conf.get(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF);

      LOG.info("Committing job has started for table: {}, using location: {}",
          table, generateJobLocation(outputTable.table.location(), conf, jobContext.getJobID()));

      int numTasks = SessionStateUtil.getCommitInfo(conf, name).map(info -> info.get(jobContext.getJobID().toString()))
          .map(SessionStateUtil.CommitInfo::getTaskNum).orElseGet(() -> {
            // Fallback logic, if number of tasks are not available in the config
            // If there are reducers, then every reducer will generate a result file.
            // If this is a map only task, then every mapper will generate a result file.
            LOG.info("Number of tasks not available in session state for jobID: {}, table: {}. " +
                "Falling back to jobConf numReduceTasks/numMapTasks", jobContext.getJobID(), name);
            return conf.getNumReduceTasks() > 0 ? conf.getNumReduceTasks() : conf.getNumMapTasks();
          });

      FilesForCommit writeResults = collectResults(
          numTasks, executor, outputTable.table.location(), jobContext, io, true);
      dataFiles.addAll(writeResults.dataFiles());
      deleteFiles.addAll(writeResults.deleteFiles());
      referencedDataFiles.addAll(writeResults.referencedDataFiles());
    }

    FilesForCommit filesForCommit = new FilesForCommit(dataFiles, deleteFiles, referencedDataFiles);
    long startTime = System.currentTimeMillis();

    if (Operation.IOW != operation) {
      if (filesForCommit.isEmpty()) {
        LOG.info(
            "Not creating a new commit for table: {}, jobIDs: {}, since there were no new files to add",
            table, outputTable.jobContexts.stream().map(JobContext::getJobID)
                .map(String::valueOf).collect(Collectors.joining(",")));
      } else {
        Long snapshotId = getSnapshotId(outputTable.table, branchName);
        commitWrite(table, branchName, snapshotId, startTime, filesForCommit, operation);
      }
    } else {

      RewritePolicy rewritePolicy = RewritePolicy.fromString(outputTable.jobContexts.stream()
          .findAny()
          .map(x -> x.getJobConf().get(ConfVars.REWRITE_POLICY.varname))
          .orElse(RewritePolicy.DEFAULT.name()));

      commitOverwrite(table, branchName, startTime, filesForCommit, rewritePolicy);
    }
  }

  private Long getSnapshotId(Table table, String branchName) {
    Optional<Long> snapshotId = Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    if (StringUtils.isNotEmpty(branchName)) {
      String ref = HiveUtils.getTableSnapshotRef(branchName);
      snapshotId = Optional.ofNullable(table.refs().get(ref)).map(SnapshotRef::snapshotId);
    }
    return snapshotId.orElse(null);
  }

  /**
   * Creates and commits an Iceberg change with the provided data and delete files.
   * If there are no delete files then an Iceberg 'append' is created, otherwise Iceberg 'overwrite' is created.
   * @param table The table we are changing
   * @param startTime The start time of the commit - used only for logging
   * @param results The object containing the new files we would like to add to the table
   */
  private void commitWrite(Table table, String branchName, Long snapshotId, long startTime,
      FilesForCommit results, Operation operation) {

    if (!results.referencedDataFiles().isEmpty()) {
      OverwriteFiles write = table.newOverwrite();
      results.referencedDataFiles().forEach(write::deleteFile);
      results.dataFiles().forEach(write::addFile);

      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      if (snapshotId != null) {
        write.validateFromSnapshot(snapshotId);
      }
      write.validateNoConflictingData();
      write.commit();
      return;
    }

    if (results.deleteFiles().isEmpty() && Operation.MERGE != operation) {
      AppendFiles write = table.newAppend();
      results.dataFiles().forEach(write::appendFile);
      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      write.commit();

    } else {
      RowDelta write = table.newRowDelta();
      results.dataFiles().forEach(write::addRows);
      results.deleteFiles().forEach(write::addDeletes);
      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      if (snapshotId != null) {
        write.validateFromSnapshot(snapshotId);
      }
      if (!results.dataFiles().isEmpty()) {
        write.validateDeletedFiles();
        write.validateNoConflictingDeleteFiles();
      }
      write.validateNoConflictingDataFiles();
      write.commit();
    }

    LOG.info("Write commit took {} ms for table: {} with {} data and {} delete file(s)",
        System.currentTimeMillis() - startTime, table, results.dataFiles().size(), results.deleteFiles().size());
    LOG.debug("Added files {}", results);
  }

  /**
   * Creates and commits an Iceberg insert overwrite change with the provided data files.
   * For unpartitioned tables the table content is replaced with the new data files. If not data files are provided
   * then the unpartitioned table is truncated.
   * For partitioned tables the relevant partitions are replaced with the new data files. If no data files are provided
   * then the unpartitioned table remains unchanged.
   * @param table The table we are changing
   * @param startTime The start time of the commit - used only for logging
   * @param results The object containing the new files
   * @param rewritePolicy The rewrite policy to use for the insert overwrite commit
   */
  private void commitOverwrite(Table table, String branchName, long startTime, FilesForCommit results,
      RewritePolicy rewritePolicy) {
    Preconditions.checkArgument(results.deleteFiles().isEmpty(), "Can not handle deletes with overwrite");
    if (!results.dataFiles().isEmpty()) {
      Transaction transaction = table.newTransaction();
      if (rewritePolicy == RewritePolicy.ALL_PARTITIONS) {
        DeleteFiles delete = transaction.newDelete();
        delete.deleteFromRowFilter(Expressions.alwaysTrue());
        delete.commit();
      }
      ReplacePartitions overwrite = transaction.newReplacePartitions();
      results.dataFiles().forEach(overwrite::addFile);
      if (StringUtils.isNotEmpty(branchName)) {
        overwrite.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      overwrite.commit();
      transaction.commitTransaction();
      LOG.info("Overwrite commit took {} ms for table: {} with {} file(s)", System.currentTimeMillis() - startTime,
          table, results.dataFiles().size());
    } else if (table.spec().isUnpartitioned()) {
      DeleteFiles deleteFiles = table.newDelete();
      deleteFiles.deleteFromRowFilter(Expressions.alwaysTrue());
      if (StringUtils.isNotEmpty(branchName)) {
        deleteFiles.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      deleteFiles.commit();
      LOG.info("Cleared table contents as part of empty overwrite for unpartitioned table. " +
          "Commit took {} ms for table: {}", System.currentTimeMillis() - startTime, table);
    }

    LOG.debug("Overwrote partitions with files {}", results);
  }

  /**
   * Cleans up the jobs temporary locations. For every target table there is a temp dir to clean up.
   * @param jobContext The job context
   * @param jobLocations The locations to clean up
   * @throws IOException if there is a failure deleting the files
   */
  private void cleanup(JobContext jobContext, Collection<String> jobLocations) throws IOException {
    JobConf jobConf = jobContext.getJobConf();

    LOG.info("Cleaning for job {} started", jobContext.getJobID());

    // Remove the job's temp directories recursively.
    Tasks.foreach(jobLocations)
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure((jobLocation, exc) -> LOG.debug("Failed to remove directory {} on job cleanup", jobLocation, exc))
        .run(jobLocation -> {
          LOG.info("Cleaning location: {}", jobLocation);
          Path toDelete = new Path(jobLocation);
          FileSystem fs = Util.getFs(toDelete, jobConf);
          fs.delete(toDelete, true);
        }, IOException.class);

    LOG.info("Cleaning for job {} finished", jobContext.getJobID());
  }

  /**
   * Executor service for parallel handling of file reads. Should be shared when committing multiple tables.
   * @param conf The configuration containing the pool size
   * @return The generated executor service
   */
  private static ExecutorService fileExecutor(Configuration conf) {
    int size = conf.getInt(InputFormatConfig.COMMIT_FILE_THREAD_POOL_SIZE,
        InputFormatConfig.COMMIT_FILE_THREAD_POOL_SIZE_DEFAULT);
    return Executors.newFixedThreadPool(
        size,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setPriority(Thread.NORM_PRIORITY)
            .setNameFormat("iceberg-commit-file-pool-%d")
            .build());
  }

  /**
   * Executor service for parallel handling of table manipulation. Could return null, if no parallelism is possible.
   * @param conf The configuration containing the pool size
   * @param maxThreadNum The number of requests we want to handle (might be decreased further by configuration)
   * @return The generated executor service, or null if executor is not needed.
   */
  private static ExecutorService tableExecutor(Configuration conf, int maxThreadNum) {
    int size = conf.getInt(InputFormatConfig.COMMIT_TABLE_THREAD_POOL_SIZE,
        InputFormatConfig.COMMIT_TABLE_THREAD_POOL_SIZE_DEFAULT);
    size = Math.min(maxThreadNum, size);
    if (size > 1) {
      return Executors.newFixedThreadPool(
          size,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setPriority(Thread.NORM_PRIORITY)
              .setNameFormat("iceberg-commit-table-pool-%d")
              .build());
    } else {
      return null;
    }
  }

  /**
   * Get the committed data or delete files for this table and job.
   *
   * @param numTasks Number of writer tasks that produced a forCommit file
   * @param executor The executor used for reading the forCommit files parallel
   * @param location The location of the table
   * @param jobContext The job context
   * @param io The FileIO used for reading a files generated for commit
   * @param throwOnFailure If <code>true</code> then it throws an exception on failure
   * @return The list of the write results, which include the committed data or delete files
   */
  private static FilesForCommit collectResults(int numTasks, ExecutorService executor, String location,
      JobContext jobContext, FileIO io, boolean throwOnFailure) {
    JobConf conf = jobContext.getJobConf();
    // Reading the committed files. The assumption here is that the taskIds are generated in sequential order
    // starting from 0.
    Collection<DataFile> dataFiles = new ConcurrentLinkedQueue<>();
    Collection<DeleteFile> deleteFiles = new ConcurrentLinkedQueue<>();
    Collection<DataFile> referencedDataFiles = new ConcurrentLinkedQueue<>();
    Tasks.range(numTasks)
        .throwFailureWhenFinished(throwOnFailure)
        .executeWith(executor)
        .retry(3)
        .run(taskId -> {
          String taskFileName = generateFileForCommitLocation(location, conf, jobContext.getJobID(), taskId);
          FilesForCommit files = readFileForCommit(taskFileName, io);
          dataFiles.addAll(files.dataFiles());
          deleteFiles.addAll(files.deleteFiles());
          referencedDataFiles.addAll(files.referencedDataFiles());

        });

    return new FilesForCommit(dataFiles, deleteFiles, referencedDataFiles);
  }

  /**
   * Generates the job temp location based on the job configuration.
   * Currently it uses TABLE_LOCATION/temp/QUERY_ID-jobId.
   * @param location The location of the table
   * @param conf The job's configuration
   * @param jobId The JobID for the task
   * @return The file to store the results
   */
  @VisibleForTesting
  static String generateJobLocation(String location, Configuration conf, JobID jobId) {
    String queryId = conf.get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    return location + "/temp/" + queryId + "-" + jobId;
  }

  /**
   * Generates file location based on the task configuration and a specific task id.
   * This file will be used to store the data required to generate the Iceberg commit.
   * Currently it uses TABLE_LOCATION/temp/QUERY_ID-jobId/task-[0..numTasks).forCommit.
   * @param location The location of the table
   * @param conf The job's configuration
   * @param jobId The jobId for the task
   * @param taskId The taskId for the commit file
   * @return The file to store the results
   */
  private static String generateFileForCommitLocation(String location, Configuration conf, JobID jobId, int taskId) {
    return generateJobLocation(location, conf, jobId) + "/task-" + taskId + FOR_COMMIT_EXTENSION;
  }

  private static void createFileForCommit(FilesForCommit writeResult, String location, FileIO io) throws IOException {
    OutputFile fileForCommit = io.newOutputFile(location);
    try (ObjectOutputStream oos = new ObjectOutputStream(fileForCommit.createOrOverwrite())) {
      oos.writeObject(writeResult);
    }
    LOG.debug("Iceberg committed file is created {}", fileForCommit);
  }

  private static FilesForCommit readFileForCommit(String fileForCommitLocation, FileIO io) {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(fileForCommitLocation).newStream())) {
      return (FilesForCommit) ois.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse committed file: %s", fileForCommitLocation);
    }
  }
}
