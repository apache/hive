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
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.compaction.IcebergCompactionService;
import org.apache.iceberg.mr.hive.compaction.IcebergCompactionUtil;
import org.apache.iceberg.mr.hive.writer.HiveIcebergWriter;
import org.apache.iceberg.mr.hive.writer.WriterRegistry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Iceberg table committer for adding data files to the Iceberg tables.
 * Currently independent of the Hive ACID transactions.
 */
public class HiveIcebergOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergOutputCommitter.class);

  private static final Splitter TABLE_NAME_SPLITTER = Splitter.on("..");
  private static final String CONFLICT_DETECTION_FILTER = "Conflict detection Filter Expression: {}";

  private ExecutorService workerPool;

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
    Set<Path> mergedPaths = getCombinedLocations(jobConf);
    Set<String> outputs = outputTables(context.getJobConf());

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
            Table table = HiveTableUtil.deserializeTable(context.getJobConf(), output);
            if (table != null) {
              String fileForCommitLocation = HiveTableUtil.fileForCommitLocation(table.location(), jobConf,
                  attemptID.getJobID(), attemptID.getTaskID().getId());
              if (writers.get(output) != null) {
                List<DataFile> dataFiles = Lists.newArrayList();
                List<DeleteFile> deleteFiles = Lists.newArrayList();
                List<DataFile> replacedDataFiles = Lists.newArrayList();
                List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();
                Set<CharSequence> referencedDataFiles = Sets.newHashSet();

                for (HiveIcebergWriter writer : writers.get(output)) {
                  FilesForCommit files = writer.files();
                  dataFiles.addAll(files.dataFiles());
                  deleteFiles.addAll(files.deleteFiles());
                  replacedDataFiles.addAll(files.replacedDataFiles());
                  referencedDataFiles.addAll(files.referencedDataFiles());
                  rewrittenDeleteFiles.addAll(files.rewrittenDeleteFiles());
                }
                createFileForCommit(
                    new FilesForCommit(dataFiles, deleteFiles, replacedDataFiles, referencedDataFiles,
                        rewrittenDeleteFiles, mergedPaths), fileForCommitLocation, table.io());
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
   * Receives a custom workerPool to be used for SnapshotUpdate.commit() operations.
   * @param workerPool to pass to SnapshotUpdates
   */
  public void setWorkerPool(ExecutorService workerPool) {
    this.workerPool = workerPool;
  }

  /**
     * Wrapper class for storing output {@link Table} and it's context for committing changes:
     * JobContext, CommitInfo.
     */
  private record OutputTable(String catalogName, String tableName, Table table) {

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
    Multimap<OutputTable, JobContext> outputs = collectOutputs(jobContextList);
    JobConf jobConf = jobContextList.getFirst().getJobConf();
    long startTime = System.currentTimeMillis();

    String ids = jobContextList.stream()
        .map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(","));
    LOG.info("Committing job(s) {} has started", ids);

    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();
    try (ExecutorService fileExecutor = fileExecutor(jobConf);
         ExecutorService tableExecutor = tableExecutor(jobConf, outputs.keySet().size())) {
      // Commits the changes for the output tables in parallel
      Tasks.foreach(outputs.keySet())
          .throwFailureWhenFinished()
          .stopOnFailure()
          .executeWith(tableExecutor)
          .run(output -> {
            final Collection<JobContext> jobContexts = outputs.get(output);
            final Table table = output.table;
            jobContexts.forEach(jobContext -> jobLocations.add(
                HiveTableUtil.jobLocation(table.location(), jobConf, jobContext.getJobID()))
            );
            commitTable(table.io(), fileExecutor, output, jobContexts, operation);
          });

      // Cleanup any merge input files.
      cleanMergeTaskInputFiles(jobContextList, fileExecutor);
    }

    LOG.info("Commit took {} ms for job(s) {}", System.currentTimeMillis() - startTime, ids);
    for (JobContext jobContext : jobContextList) {
      cleanup(jobContext, jobLocations);
    }
  }

  private static Multimap<OutputTable, JobContext> collectOutputs(List<JobContext> jobContextList) {
    Multimap<OutputTable, JobContext> outputs =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (JobContext jobContext : jobContextList) {
      for (String output : outputTables(jobContext.getJobConf())) {
        Table table = SessionStateUtil.getResource(jobContext.getJobConf(), output)
            .filter(o -> o instanceof Table).map(o -> (Table) o)
            // fall back to getting the serialized table from the config
            .orElseGet(() -> HiveTableUtil.deserializeTable(jobContext.getJobConf(), output));
        if (table != null) {
          String catalogName = catalogName(jobContext.getJobConf(), output);
          outputs.put(new OutputTable(catalogName, output, table), jobContext);
        } else {
          LOG.info("Found no table object in QueryState or conf for: {}. Skipping job commit.", output);
        }
      }
    }
    return outputs;
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
    Multimap<OutputTable, JobContext> outputs = collectOutputs(jobContextList);
    JobConf jobConf = jobContextList.getFirst().getJobConf();

    String ids = jobContextList.stream()
        .map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(","));
    LOG.info("Job(s) {} are aborted. Data file cleaning started", ids);

    ExecutorService fileExecutor = fileExecutor(jobConf);
    ExecutorService tableExecutor = tableExecutor(jobConf, outputs.keySet().size());

    Collection<String> jobLocations = new ConcurrentLinkedQueue<>();
    try {
      // Cleans up the changes for the output tables in parallel
      Tasks.foreach(outputs.keySet())
          .suppressFailureWhenFinished()
          .executeWith(tableExecutor)
          .onFailure((output, ex) -> LOG.warn("Failed cleanup table {} on abort job", output, ex))
          .run(output -> {
            for (JobContext jobContext : outputs.get(output)) {
              LOG.info("Cleaning job for jobID: {}, table: {}", jobContext.getJobID(), output);
              Table table = output.table;
              String jobLocation = HiveTableUtil.jobLocation(table.location(), jobConf, jobContext.getJobID());
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
                  .onFailure((file, ex) -> LOG.warn("Failed to remove data file {} on abort job", file.location(), ex))
                  .run(file -> table.io().deleteFile(file.location()));
            }
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
  private static Set<FileStatus> listForCommits(JobConf jobConf, String jobLocation) throws IOException {
    Path path = new Path(jobLocation);
    LOG.debug("Listing job location to get commitTask manifest files for abort: {}", jobLocation);
    FileStatus[] children = path.getFileSystem(jobConf).listStatus(path);
    LOG.debug("Listing the job location: {} yielded these files: {}", jobLocation, Arrays.toString(children));
    return Arrays.stream(children)
        .filter(child -> !child.isDirectory() &&
            child.getPath().getName().endsWith(HiveTableUtil.FOR_COMMIT_EXTENSION))
        .collect(Collectors.toSet());
  }

  /**
   * Collects the additions to a single table and adds/commits the new files to the Iceberg table.
   * @param io The io to read the forCommit files
   * @param executor The executor used to read the forCommit files
   * @param outputTable The table used for loading from the catalog
   */
  private void commitTable(FileIO io, ExecutorService executor, OutputTable outputTable,
      Collection<JobContext> jobContexts, Operation operation) {
    String name = outputTable.tableName;

    Properties catalogProperties = new Properties();
    catalogProperties.put(Catalogs.NAME, name);
    catalogProperties.put(Catalogs.LOCATION, outputTable.table.location());

    if (outputTable.catalogName != null) {
      catalogProperties.put(InputFormatConfig.CATALOG_NAME, outputTable.catalogName);
    }
    List<DataFile> dataFiles = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    List<DataFile> replacedDataFiles = Lists.newArrayList();
    List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();
    Set<CharSequence> referencedDataFiles = Sets.newHashSet();
    Set<Path> mergedAndDeletedFiles = Sets.newHashSet();

    Table table = null;
    String branchName = null;
    Long snapshotId = null;
    Expression filterExpr = null;

    for (JobContext jobContext : jobContexts) {
      JobConf conf = jobContext.getJobConf();

      table = Optional.ofNullable(table).orElseGet(() -> Catalogs.loadTable(conf, catalogProperties));
      branchName = conf.get(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF);
      snapshotId = getSnapshotId(outputTable.table, branchName);

      if (filterExpr == null) {
        filterExpr = SessionStateUtil.getConflictDetectionFilter(conf, catalogProperties.get(Catalogs.NAME))
            .map(expr -> HiveIcebergInputFormat.getFilterExpr(conf, expr))
            .orElse(null);
      }

      LOG.info("Committing job has started for table: {}, using location: {}",
          table, HiveTableUtil.jobLocation(outputTable.table.location(), conf, jobContext.getJobID()));

      int numTasks = SessionStateUtil.getCommitInfo(conf, name)
          .map(info -> info.get(jobContext.getJobID().toString()))
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
      replacedDataFiles.addAll(writeResults.replacedDataFiles());
      referencedDataFiles.addAll(writeResults.referencedDataFiles());
      rewrittenDeleteFiles.addAll(writeResults.rewrittenDeleteFiles());

      mergedAndDeletedFiles.addAll(writeResults.mergedAndDeletedFiles());
    }

    dataFiles.removeIf(dataFile -> mergedAndDeletedFiles.contains(new Path(dataFile.location())));
    deleteFiles.removeIf(deleteFile -> mergedAndDeletedFiles.contains(new Path(deleteFile.location())));

    FilesForCommit filesForCommit =
        new FilesForCommit(dataFiles, deleteFiles, replacedDataFiles, referencedDataFiles, rewrittenDeleteFiles,
            Collections.emptySet());
    long startTime = System.currentTimeMillis();

    if (Operation.IOW != operation) {
      if (filesForCommit.isEmpty()) {
        LOG.info(
            "Not creating a new commit for table: {}, jobIDs: {}, since there were no new files to add",
            table, jobContexts.stream().map(JobContext::getJobID)
                .map(String::valueOf).collect(Collectors.joining(",")));
      } else {
        commitWrite(table, branchName, snapshotId, startTime, filesForCommit, operation, filterExpr);
      }
    } else {
      RewritePolicy rewritePolicy = RewritePolicy.fromString(jobContexts.stream()
          .findAny()
          .map(x -> x.getJobConf().get(ConfVars.REWRITE_POLICY.varname))
          .orElse(RewritePolicy.DEFAULT.name()));

      if (rewritePolicy != RewritePolicy.DEFAULT) {
        String partitionPath = jobContexts.stream()
            .findAny()
            .map(x -> x.getJobConf().get(IcebergCompactionService.PARTITION_PATH))
            .orElse(null);

        long fileSizeThreshold = jobContexts.stream()
            .findAny()
            .map(x -> x.getJobConf().get(CompactorContext.COMPACTION_FILE_SIZE_THRESHOLD))
            .map(Long::parseLong)
            .orElse(-1L);

        commitCompaction(table, snapshotId, startTime, filesForCommit, partitionPath, fileSizeThreshold);
      } else {
        commitOverwrite(table, branchName, snapshotId, startTime, filesForCommit);
      }
    }
  }

  private Long getSnapshotId(Table table, String branchName) {
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, branchName);
    return (snapshot != null) ? snapshot.snapshotId() : null;
  }

  /**
   * Creates and commits an Iceberg change with the provided data and delete files.
   * If there are no delete files then an Iceberg 'append' is created, otherwise Iceberg 'overwrite' is created.
   * @param table      The table we are changing
   * @param snapshotId The snapshot id of the table to use for validation
   * @param startTime  The start time of the commit - used only for logging
   * @param results    The object containing the new files we would like to add to the table
   * @param filterExpr Filter expression for conflict detection filter
   */
  private void commitWrite(Table table, String branchName, Long snapshotId, long startTime,
      FilesForCommit results, Operation operation, Expression filterExpr) {

    if (!results.replacedDataFiles().isEmpty()) {
      OverwriteFiles write = table.newOverwrite();
      results.replacedDataFiles().forEach(write::deleteFile);
      results.dataFiles().forEach(write::addFile);

      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      if (snapshotId != null) {
        write.validateFromSnapshot(snapshotId);
      }
      if (filterExpr != null) {
        LOG.debug(CONFLICT_DETECTION_FILTER, filterExpr);
        write.conflictDetectionFilter(filterExpr);
      }
      write.validateNoConflictingData();
      write.validateNoConflictingDeletes();
      commit(write);
      return;
    }

    if (results.deleteFiles().isEmpty() && Operation.MERGE != operation) {
      AppendFiles write = table.newAppend();
      results.dataFiles().forEach(write::appendFile);
      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      commit(write);

    } else {
      RowDelta write = table.newRowDelta();
      results.dataFiles().forEach(write::addRows);
      results.deleteFiles().forEach(write::addDeletes);
      results.rewrittenDeleteFiles().forEach(write::removeDeletes);

      if (StringUtils.isNotEmpty(branchName)) {
        write.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      if (snapshotId != null) {
        write.validateFromSnapshot(snapshotId);
      }
      if (filterExpr != null) {
        LOG.debug(CONFLICT_DETECTION_FILTER, filterExpr);
        write.conflictDetectionFilter(filterExpr);
      }
      if (!results.dataFiles().isEmpty()) {
        write.validateDeletedFiles();
        write.validateNoConflictingDeleteFiles();
      }
      write.validateDataFilesExist(results.referencedDataFiles());
      write.validateNoConflictingDataFiles();
      commit(write);
    }

    LOG.info("Write commit took {} ms for table: {} with {} data and {} delete file(s)",
        System.currentTimeMillis() - startTime, table, results.dataFiles().size(), results.deleteFiles().size());
    LOG.debug("Added files {}", results);
  }

  /**
   * Calls the commit on the prepared SnapshotUpdate and supplies the ExecutorService if any.
   * @param update the SnapshotUpdate of any kind (e.g. AppendFiles, DeleteFiles, etc.)
   */
  private void commit(SnapshotUpdate<?> update) {
    if (workerPool != null) {
      update.scanManifestsWith(workerPool);
    }
    update.commit();
  }
  /**
   * Creates and commits an Iceberg compaction change with the provided data files.
   * Either full table or a selected partition contents is replaced with compacted files.
   *
   * @param table         The table we are changing
   * @param snapshotId    The snapshot id of the table to use for validation
   * @param startTime     The start time of the commit - used only for logging
   * @param results       The object containing the new files
   * @param partitionPath The path of the compacted partition
   */
  private void commitCompaction(Table table, Long snapshotId, long startTime, FilesForCommit results,
      String partitionPath, long fileSizeThreshold) {
    List<DataFile> existingDataFiles = IcebergCompactionUtil.getDataFiles(table, partitionPath, fileSizeThreshold);
    List<DeleteFile> existingDeleteFiles = fileSizeThreshold == -1 ?
        IcebergCompactionUtil.getDeleteFiles(table, partitionPath) : Collections.emptyList();

    RewriteFiles rewriteFiles = table.newRewrite();
    existingDataFiles.forEach(rewriteFiles::deleteFile);
    existingDeleteFiles.forEach(rewriteFiles::deleteFile);
    results.dataFiles().forEach(rewriteFiles::addFile);

    if (snapshotId != null) {
      rewriteFiles.validateFromSnapshot(snapshotId);
    }
    rewriteFiles.commit();
    LOG.info("Compaction commit took {} ms for table: {} partition: {} with {} file(s)",
        System.currentTimeMillis() - startTime, table, StringUtils.defaultString(partitionPath, "N/A"),
        results.dataFiles().size());
  }

  /**
   * Creates and commits an Iceberg insert overwrite change with the provided data files.
   * For unpartitioned tables the table content is replaced with the new data files. Table is truncated
   * if no data files are provided.
   * For partitioned tables the relevant partitions are replaced with the new data files. Table remains unchanged
   * unless data files are provided.
   *
   * @param table      The table we are changing
   * @param snapshotId The snapshot id of the table to use for validation
   * @param startTime  The start time of the commit - used only for logging
   * @param results    The object containing the new files
   */
  private void commitOverwrite(Table table, String branchName, Long snapshotId, long startTime,
      FilesForCommit results) {
    Preconditions.checkArgument(results.deleteFiles().isEmpty(), "Can not handle deletes with overwrite");
    if (!results.dataFiles().isEmpty()) {
      ReplacePartitions overwrite = table.newReplacePartitions();
      results.dataFiles().forEach(overwrite::addFile);

      if (StringUtils.isNotEmpty(branchName)) {
        overwrite.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      if (snapshotId != null) {
        overwrite.validateFromSnapshot(snapshotId);
      }
      overwrite.validateNoConflictingDeletes();
      overwrite.validateNoConflictingData();
      commit(overwrite);
      LOG.info("Overwrite commit took {} ms for table: {} with {} file(s)", System.currentTimeMillis() - startTime,
          table, results.dataFiles().size());
    } else if (table.spec().isUnpartitioned()) {
      DeleteFiles deleteFiles = table.newDelete();
      deleteFiles.deleteFromRowFilter(Expressions.alwaysTrue());

      if (StringUtils.isNotEmpty(branchName)) {
        deleteFiles.toBranch(HiveUtils.getTableSnapshotRef(branchName));
      }
      commit(deleteFiles);
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
        .onFailure((jobLocation, ex) -> LOG.debug("Failed to remove directory {} on job cleanup", jobLocation, ex))
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
    Collection<DataFile> replacedDataFiles = new ConcurrentLinkedQueue<>();
    Collection<DeleteFile> rewrittenDeleteFiles = new ConcurrentLinkedQueue<>();
    Collection<CharSequence> referencedDataFiles = new ConcurrentLinkedQueue<>();
    Collection<Path> mergedAndDeletedFiles = new ConcurrentLinkedQueue<>();
    Tasks.range(numTasks)
        .throwFailureWhenFinished(throwOnFailure)
        .executeWith(executor)
        .retry(3)
        .run(taskId -> {
          String taskFileName = HiveTableUtil.fileForCommitLocation(location, conf, jobContext.getJobID(), taskId);
          final FilesForCommit files = readFileForCommit(taskFileName, io);
          LOG.debug("Found Iceberg commitTask manifest file: {}\n{}", taskFileName, files);

          dataFiles.addAll(files.dataFiles());
          deleteFiles.addAll(files.deleteFiles());
          replacedDataFiles.addAll(files.replacedDataFiles());
          rewrittenDeleteFiles.addAll(files.rewrittenDeleteFiles());
          referencedDataFiles.addAll(files.referencedDataFiles());
          mergedAndDeletedFiles.addAll(files.mergedAndDeletedFiles());
        });

    return new FilesForCommit(dataFiles, deleteFiles, replacedDataFiles, referencedDataFiles, rewrittenDeleteFiles,
        mergedAndDeletedFiles);
  }

  private static void createFileForCommit(FilesForCommit writeResult, String location, FileIO io) throws IOException {
    OutputFile fileForCommit = io.newOutputFile(location);
    try (ObjectOutputStream oos = new ObjectOutputStream(fileForCommit.createOrOverwrite())) {
      oos.writeObject(writeResult);
    }
    LOG.debug("Created Iceberg commitTask manifest file: {}\n{}", location, writeResult);
  }

  private static FilesForCommit readFileForCommit(String fileForCommitLocation, FileIO io) {
    try (ObjectInputStream ois = new ObjectInputStream(io.newInputFile(fileForCommitLocation).newStream())) {
      return (FilesForCommit) ois.readObject();
    } catch (ClassNotFoundException | IOException e) {
      throw new NotFoundException("Can not read or parse commitTask manifest file: %s", fileForCommitLocation);
    }
  }

  /**
   * Generates a list of file statuses of the output files in the jobContexts.
   * @param jobContexts List of jobContexts
   * @return Returns the list of file statuses of the output files in the jobContexts
   * @throws IOException Throws IOException
   */
  public static List<FileStatus> getOutputFiles(List<JobContext> jobContexts) throws IOException {
    Multimap<OutputTable, JobContext> outputs = collectOutputs(jobContexts);
    JobConf jobConf = jobContexts.getFirst().getJobConf();

    Map<Path, List<FileStatus>> parentDirToDataFile = Maps.newConcurrentMap();
    Map<Path, List<FileStatus>> parentDirToDeleteFile = Maps.newConcurrentMap();

    try (ExecutorService fileExecutor = fileExecutor(jobConf);
         ExecutorService tableExecutor = tableExecutor(jobConf, outputs.keySet().size())) {

      Tasks.foreach(outputs.keySet())
          .suppressFailureWhenFinished()
          .executeWith(tableExecutor)
          .onFailure((output, ex) -> LOG.warn("Failed to retrieve merge input file for the table {}", output, ex))
          .run(output -> {
            for (JobContext jobContext : outputs.get(output)) {
              Table table = output.table;
              FileSystem fileSystem = new Path(table.location()).getFileSystem(jobConf);
              String jobLocation = HiveTableUtil.jobLocation(table.location(), jobConf, jobContext.getJobID());
              // list jobLocation to get number of forCommit files
              // we do this because map/reduce num in jobConf is unreliable
              // and we have no access to vertex status info
              int numTasks = listForCommits(jobConf, jobLocation).size();
              FilesForCommit results = collectResults(numTasks, fileExecutor, table.location(), jobContext,
                  table.io(), false);
              for (DataFile dataFile : results.dataFiles()) {
                Path filePath = new Path(dataFile.location());
                parentDirToDataFile.computeIfAbsent(filePath.getParent(), k -> Lists.newArrayList())
                    .add(fileSystem.getFileStatus(filePath));
              }
              for (DeleteFile deleteFile : results.deleteFiles()) {
                Path filePath = new Path(deleteFile.location());
                parentDirToDeleteFile.computeIfAbsent(filePath.getParent(), k -> Lists.newArrayList())
                    .add(fileSystem.getFileStatus(filePath));
              }
            }
          }, IOException.class);
    }
    return Stream.of(parentDirToDataFile, parentDirToDeleteFile)
      .flatMap(files ->
          files.values().stream().flatMap(List::stream))
      .collect(Collectors.toList());
  }

  /**
   * Generates a list of ContentFile objects of the output files in the jobContexts.
   * @param jobContexts List of jobContexts
   * @return Returns the list of file statuses of the output files in the jobContexts
   * @throws IOException Throws IOException
   */
  public static List<ContentFile<?>> getOutputContentFiles(List<JobContext> jobContexts) throws IOException {
    Multimap<OutputTable, JobContext> outputs = collectOutputs(jobContexts);
    JobConf jobConf = jobContexts.getFirst().getJobConf();

    Collection<ContentFile<?>> files = new ConcurrentLinkedQueue<>();

    try (ExecutorService fileExecutor = fileExecutor(jobConf);
        ExecutorService tableExecutor = tableExecutor(jobConf, outputs.keySet().size())) {

      Tasks.foreach(outputs.keySet())
          .suppressFailureWhenFinished()
          .executeWith(tableExecutor)
          .onFailure((output, ex) -> LOG.warn("Failed to retrieve merge input file for the table {}", output, ex))
          .run(output -> {
            for (JobContext jobContext : outputs.get(output)) {
              Table table = output.table;
              String jobLocation = HiveTableUtil.jobLocation(table.location(), jobConf, jobContext.getJobID());
              // list jobLocation to get number of forCommit files
              // we do this because map/reduce num in jobConf is unreliable
              // and we have no access to vertex status info
              int numTasks = listForCommits(jobConf, jobLocation).size();
              FilesForCommit results = collectResults(numTasks, fileExecutor, table.location(), jobContext,
                  table.io(), false);
              files.addAll(results.dataFiles());
              files.addAll(results.deleteFiles());
            }
          }, IOException.class);
    }
    return Lists.newArrayList(files);
  }

  private void cleanMergeTaskInputFiles(List<JobContext> jobContexts, ExecutorService fileExecutor) throws IOException {
    // Merge task has merged several files into one. Hence we need to remove the stale files.
    // At this stage the file is written and task-committed, but the old files are still present.
    Stream<Path> mergedPaths = jobContexts.stream()
        .map(JobContext::getJobConf)
        .filter(jobConf -> jobConf.getInputFormat().getClass().isAssignableFrom(CombineHiveInputFormat.class))
        .map(Utilities::getMapWork).filter(Objects::nonNull)
        .map(MapWork::getInputPaths).filter(Objects::nonNull)
        .flatMap(List::stream);

    Tasks.foreach(mergedPaths)
        .retry(3)
        .executeWith(fileExecutor)
        .run(path -> {
          FileSystem fs = path.getFileSystem(jobContexts.getFirst().getJobConf());
          if (fs.exists(path)) {
            fs.delete(path, true);
          }
        }, IOException.class);
  }

  static List<JobContext> getJobContexts(Properties properties) {
    Configuration configuration = SessionState.getSessionConf();
    String tableName = properties.getProperty(Catalogs.NAME);
    String snapshotRef = properties.getProperty(Catalogs.SNAPSHOT_REF);

    return generateJobContexts(configuration, tableName, snapshotRef)
        .stream()
        .map(TezUtil::enrichContextWithVertexId)
        .toList();
  }

  /**
   * Generates {@link JobContext}s for the OutputCommitter for the specific table.
   * @param configuration The configuration used for as a base of the JobConf
   * @param tableName The name of the table we are planning to commit
   * @param branchName the name of the branch
   * @return The generated Optional JobContext list or empty if not presents.
   */
  static List<JobContext> generateJobContexts(Configuration configuration, String tableName, String branchName) {
    JobConf jobConf = new JobConf(configuration);
    Optional<Map<String, SessionStateUtil.CommitInfo>> commitInfoMap =
        SessionStateUtil.getCommitInfo(jobConf, tableName);
    if (commitInfoMap.isPresent()) {
      List<JobContext> jobContextList = Lists.newLinkedList();
      for (SessionStateUtil.CommitInfo commitInfo : commitInfoMap.get().values()) {
        org.apache.hadoop.mapred.JobID jobID = org.apache.hadoop.mapred.JobID.forName(commitInfo.getJobIdStr());
        commitInfo.getProps().forEach(jobConf::set);

        // we should only commit this current table because
        // for multi-table inserts, this hook method will be called sequentially for each target table
        jobConf.set(InputFormatConfig.OUTPUT_TABLES, tableName);
        if (branchName != null) {
          jobConf.set(InputFormatConfig.OUTPUT_TABLE_SNAPSHOT_REF, branchName);
        }
        jobContextList.add(new JobContextImpl(jobConf, jobID, null));
      }
      return jobContextList;
    } else {
      // most likely empty write scenario
      LOG.debug("Unable to find commit information in query state for table: {}", tableName);
      return Collections.emptyList();
    }
  }

  private static Set<Path> getCombinedLocations(JobConf jobConf) {
    Set<Path> mergedPaths = Sets.newHashSet();
    if (jobConf.getInputFormat().getClass().isAssignableFrom(CombineHiveInputFormat.class)) {
      MapWork mrwork = Utilities.getMapWork(jobConf);
      if (mrwork != null && mrwork.getInputPaths() != null) {
        mergedPaths.addAll(mrwork.getInputPaths());
      }
    }
    return mergedPaths;
  }

  /**
   * Returns the names of the output tables stored in the configuration.
   * @param config The configuration used to get the data from
   * @return The collection of the table names as returned by TableDesc.getTableName()
   */
  private static Set<String> outputTables(Configuration config) {
    return Sets.newHashSet(TABLE_NAME_SPLITTER.split(config.get(InputFormatConfig.OUTPUT_TABLES)));
  }

  /**
   * Returns the catalog name serialized to the configuration.
   * @param config The configuration used to get the data from
   * @param name The name of the table we neeed as returned by TableDesc.getTableName()
   * @return catalog name
   */
  private static String catalogName(Configuration config, String name) {
    return config.get(InputFormatConfig.TABLE_CATALOG_PREFIX + name);
  }
}
