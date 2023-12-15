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
package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Fast file merge operator for ORC and RCfile. This is an abstract class which
 * does not process any rows. Refer {@link org.apache.hadoop.hive.ql.exec.OrcFileMergeOperator}
 * or {@link org.apache.hadoop.hive.ql.exec.RCFileMergeOperator} for more details.
 */
public abstract class AbstractFileMergeOperator<T extends FileMergeDesc>
    extends Operator<T> implements Serializable {

  public static final String BACKUP_PREFIX = "_backup.";
  public static final String UNION_SUDBIR_PREFIX = "HIVE_UNION_SUBDIR_";
  public static final Logger LOG = LoggerFactory.getLogger(AbstractFileMergeOperator.class);

  protected JobConf jc;
  protected FileSystem fs;
  private boolean autoDelete;
  private Path outPath; // The output path used by the subclasses.
  private Path finalPath; // Used as a final destination; same as outPath for MM tables.
  private Path dpPath;
  private Path tmpPath; // Only stored to update based on the original in fixTmpPath.
  private Path taskTmpPath; // Only stored to update based on the original in fixTmpPath.
  private int listBucketingDepth;
  private boolean hasDynamicPartitions;
  private boolean isListBucketingAlterTableConcatenate;
  private boolean tmpPathFixedConcatenate;
  private boolean tmpPathFixed;
  private Set<Path> incompatFileSet;
  private transient DynamicPartitionCtx dpCtx;
  private boolean isMmTable;
  private String taskId;

  /** Kryo ctor. */
  protected AbstractFileMergeOperator() {
    super();
  }

  public AbstractFileMergeOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    this.jc = new JobConf(hconf);
    incompatFileSet = new HashSet<Path>();
    autoDelete = false;
    tmpPathFixed = false;
    tmpPathFixedConcatenate = false;
    dpPath = null;
    dpCtx = conf.getDpCtx();
    hasDynamicPartitions = conf.hasDynamicPartitions();
    isListBucketingAlterTableConcatenate = conf
        .isListBucketingAlterTableConcatenate();
    listBucketingDepth = conf.getListBucketingDepth();
    Path specPath = conf.getOutputPath();
    isMmTable = conf.getIsMmTable();
    if (isMmTable) {
      updatePaths(specPath, null);
    } else {
      updatePaths(Utilities.toTempPath(specPath), Utilities.toTaskTempPath(specPath));
    }
    try {
      fs = specPath.getFileSystem(hconf);
      if (!isMmTable) {
        // Do not delete for MM tables. We either want the file if we succeed, or we must
        // delete is explicitly before proceeding if the merge fails.
        autoDelete = fs.deleteOnExit(outPath);
      }
    } catch (IOException e) {
      throw new HiveException("Failed to initialize AbstractFileMergeOperator", e);
    }
  }

  // sets up temp and task temp path
  private void updatePaths(Path tp, Path ttp) {
    taskId = Utilities.getTaskId(jc);
    tmpPath = tp;
    if (isMmTable) {
      taskTmpPath = null;
      // Make sure we don't collide with the source.
      outPath = finalPath = new Path(tmpPath, taskId + ".merged");
    } else {
      taskTmpPath = ttp;
      finalPath = new Path(tp, taskId);
      outPath = new Path(ttp, Utilities.toTempPath(taskId));
    }
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Paths for merge " + taskId + ": tmp " + tmpPath + ", task "
          + taskTmpPath + ", final " + finalPath + ", out " + outPath);
    }
  }

  /**
   * Fixes tmpPath to point to the correct partition. Initialize operator will
   * set tmpPath and taskTmpPath based on root table directory. So initially,
   * tmpPath will be <prefix>/_tmp.-ext-10000 and taskTmpPath will be
   * <prefix>/_task_tmp.-ext-10000. The depth of these two paths will be 0.
   * Now, in case of dynamic partitioning or list bucketing the inputPath will
   * have additional sub-directories under root table directory. This function
   * updates the tmpPath and taskTmpPath to reflect these additional
   * subdirectories. It updates tmpPath and taskTmpPath in the following way
   * 1. finds out the difference in path based on depthDiff provided
   * and saves the path difference in newPath
   * 2. newPath is used to update the existing tmpPath and taskTmpPath similar
   * to the way initializeOp() does.
   *
   * Note: The path difference between inputPath and tmpDepth can be DP or DP+LB.
   * This method will automatically handle it.
   *
   * Continuing the example above, if inputPath is <prefix>/-ext-10000/hr=a1/,
   * newPath will be hr=a1/. Then, tmpPath and taskTmpPath will be updated to
   * <prefix>/-ext-10000/hr=a1/_tmp.ext-10000 and
   * <prefix>/-ext-10000/hr=a1/_task_tmp.ext-10000 respectively.
   * We have list_bucket_dml_6.q cover this case: DP + LP + multiple skewed
   * values + merge.
   *
   * @param inputPath - input path
   * @throws java.io.IOException
   */
  protected void fixTmpPath(Path inputPath, int depthDiff) throws IOException {

    // don't need to update tmp paths when there is no depth difference in paths
    if (depthDiff <= 0) {
      return;
    }

    dpPath = inputPath;
    Path newPath = new Path(".");

    // Build the path from bottom up
    while (inputPath != null && depthDiff > 0) {
      newPath = new Path(inputPath.getName(), newPath);
      depthDiff--;
      inputPath = inputPath.getParent();
    }

    Path newTmpPath = new Path(tmpPath, newPath);
    if (!fs.exists(newTmpPath)) {
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Creating " + newTmpPath);
      }
      fs.mkdirs(newTmpPath);
    }

    Path newTaskTmpPath = (taskTmpPath != null) ? new Path(taskTmpPath, newPath) : null;
    updatePaths(newTmpPath, newTaskTmpPath);
  }

  /**
   * Validates that each input path belongs to the same partition since each
   * mapper merges the input to a single output directory
   *
   * @param inputPath - input path
   */
  protected void checkPartitionsMatch(Path inputPath) throws IOException {
    if (!dpPath.equals(inputPath)) {
      // Temp partition input path does not match exist temp path
      String msg = "Multiple partitions for one merge mapper: " + dpPath +
          " NOT EQUAL TO "
          + inputPath;
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  protected void fixTmpPath(Path path) throws IOException {
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Calling fixTmpPath with " + path);
    }
    // Fix temp path for alter table ... concatenate
    if (isListBucketingAlterTableConcatenate) {
      if (this.tmpPathFixedConcatenate) {
        checkPartitionsMatch(path);
      } else {
        fixTmpPath(path, listBucketingDepth);
        tmpPathFixedConcatenate = true;
      }
    } else {
      if (hasDynamicPartitions || (listBucketingDepth > 0)) {
        // In light of results from union queries, we need to be aware that
        // sub-directories can exist in the partition directory. We want to
        // ignore these sub-directories and promote merged files to the
        // partition directory.
        String name = path.getName();
        Path realPartitionPath = name.startsWith(UNION_SUDBIR_PREFIX) ? path.getParent() : path;

        if (tmpPathFixed) {
          checkPartitionsMatch(realPartitionPath);
        } else {
          // We haven't fixed the TMP path for this mapper yet
          int depthDiff = realPartitionPath.depth() - tmpPath.depth();
          fixTmpPath(realPartitionPath, depthDiff);
          tmpPathFixed = true;
        }
      }
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (abort) {
        if (!autoDelete || isMmTable) {
          fs.delete(outPath, true);
        }
        return;
      }
      // if outPath does not exist, then it means all paths within combine split are skipped as
      // they are incompatible for merge (for example: files without stripe stats).
      // Those files will be added to incompatFileSet
      if (fs.exists(outPath)) {
        FileStatus fss = fs.getFileStatus(outPath);
        if (!isMmTable) {
          if (!fs.rename(outPath, finalPath)) {
            throw new IOException("Unable to rename " + outPath + " to " + finalPath);
          }
          LOG.info("Renamed path " + outPath + " to " + finalPath
              + "(" + fss.getLen() + " bytes).");
        } else {
          assert finalPath.equals(outPath);
          // There's always just one file that we have merged.
          // The union/DP/etc. should already be account for in the path.
          Utilities.writeMmCommitManifest(Lists.newArrayList(outPath),
              tmpPath.getParent(), fs, taskId, conf.getWriteId(), conf.getStmtId(), null, false);
          LOG.info("Merged into " + finalPath + "(" + fss.getLen() + " bytes).");
        }
      }

      // move any incompatible files to final path
      if (incompatFileSet != null && !incompatFileSet.isEmpty()) {
        if (isMmTable) {
          // We only support query-time merge for MM tables, so don't handle this.
          throw new HiveException("Incompatible files should not happen in MM tables.");
        }
        Path destDir = finalPath.getParent();
        Path destPath = destDir;
        // move any incompatible files to final path
        if (incompatFileSet != null && !incompatFileSet.isEmpty()) {
          for (Path incompatFile : incompatFileSet) {
            // check if path conforms to Hive's file name convention. Hive expects filenames to be in specific format
            // like 000000_0, but "LOAD DATA" commands can let you add any files to any partitions/tables without
            // renaming. This can cause MoveTask to remove files in some cases where MoveTask assumes the files are
            // are generated by speculatively executed tasks.
            // Example: MoveTask thinks the following files are same
            // part-m-00000_1417075294718
            // part-m-00001_1417075294718
            // Assumes 1417075294718 as taskId and retains only large file supposedly generated by speculative execution.
            // This can result in data loss in case of CONCATENATE/merging. Filter out files that does not match Hive's
            // filename convention.
            if (!Utilities.isHiveManagedFile(incompatFile)) {
              // rename un-managed files to conform to Hive's naming standard
              // Example:
              // /warehouse/table/part-m-00000_1417075294718 will get renamed to /warehouse/table/.hive-staging/000000_0
              // If staging directory already contains the file, taskId_copy_N naming will be used.
              final String taskId = Utilities.getTaskId(jc);
              Path destFilePath = new Path(destDir, new Path(taskId));
              for (int counter = 1; fs.exists(destFilePath); counter++) {
                destFilePath = new Path(destDir, taskId + (Utilities.COPY_KEYWORD + counter));
              }
              LOG.warn("Path doesn't conform to Hive's expectation. Renaming {} to {}", incompatFile, destFilePath);
              destPath = destFilePath;
            }

            try {
              Utilities.renameOrMoveFiles(fs, incompatFile, destPath);
              LOG.info("Moved incompatible file " + incompatFile + " to " + destPath);
            } catch (HiveException e) {
              LOG.error("Unable to move " + incompatFile + " to " + destPath);
              throw new IOException(e);
            }
          }
        }

      }
    } catch (IOException e) {
      throw new HiveException("Failed to close AbstractFileMergeOperator", e);
    }
  }

  @Override
  public void jobCloseOp(Configuration hconf, boolean success)
      throws HiveException {
    try {
      Path outputDir = conf.getOutputPath();
      FileSystem fs = outputDir.getFileSystem(hconf);
      Long mmWriteId = conf.getWriteId();
      int stmtId = conf.getStmtId();
      if (!isMmTable) {
        Path backupPath = backupOutputPath(fs, outputDir);
        Utilities.mvFileToFinalPath(
            outputDir, hconf, success, LOG, conf.getDpCtx(), null, reporter);
        if (success) {
          LOG.info("jobCloseOp moved merged files to output dir: " + outputDir);
        }
        if (backupPath != null) {
          fs.delete(backupPath, true);
        }
      } else {
        int dpLevels = dpCtx == null ? 0 : dpCtx.getNumDPCols(),
            lbLevels = conf.getListBucketingDepth();
        // We don't expect missing buckets from mere (actually there should be no buckets),
        // so just pass null as bucketing context. Union suffix should also be accounted for.
        Utilities.handleMmTableFinalPath(outputDir.getParent(), null, hconf, success,
            dpLevels, lbLevels, null, mmWriteId, stmtId, reporter, isMmTable, false, false);
      }

    } catch (IOException e) {
      throw new HiveException("Failed jobCloseOp for AbstractFileMergeOperator",
          e);
    }
    super.jobCloseOp(hconf, success);
  }

  private Path backupOutputPath(FileSystem fs, Path outpath)
      throws IOException, HiveException {
    if (fs.exists(outpath)) {
      Path backupPath = new Path(outpath.getParent(),
          BACKUP_PREFIX + outpath.getName());
      Utilities.rename(fs, outpath, backupPath);
      return backupPath;
    } else {
      return null;
    }
  }

  @Override
  public String getName() {
    return AbstractFileMergeOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "MERGE";
  }

  protected final Path getOutPath() {
    return outPath;
  }

  protected final void addIncompatibleFile(Path path) {
    incompatFileSet.add(path);
  }
}
