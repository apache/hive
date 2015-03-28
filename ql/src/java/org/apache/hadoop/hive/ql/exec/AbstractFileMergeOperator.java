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
package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.FileMergeDesc;
import org.apache.hadoop.mapred.JobConf;

/**
 * Fast file merge operator for ORC and RCfile. This is an abstract class which
 * does not process any rows. Refer {@link org.apache.hadoop.hive.ql.exec.OrcFileMergeOperator}
 * or {@link org.apache.hadoop.hive.ql.exec.RCFileMergeOperator} for more details.
 */
public abstract class AbstractFileMergeOperator<T extends FileMergeDesc>
    extends Operator<T> implements Serializable {

  public static final String BACKUP_PREFIX = "_backup.";
  public static final Log LOG = LogFactory
      .getLog(AbstractFileMergeOperator.class);

  protected JobConf jc;
  protected FileSystem fs;
  protected boolean autoDelete;
  protected boolean exception;
  protected Path outPath;
  protected Path finalPath;
  protected Path dpPath;
  protected Path tmpPath;
  protected Path taskTmpPath;
  protected int listBucketingDepth;
  protected boolean hasDynamicPartitions;
  protected boolean isListBucketingAlterTableConcatenate;
  protected boolean tmpPathFixedConcatenate;
  protected boolean tmpPathFixed;
  protected Set<Path> incompatFileSet;
  protected transient DynamicPartitionCtx dpCtx;

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);
    this.jc = new JobConf(hconf);
    incompatFileSet = new HashSet<Path>();
    autoDelete = false;
    exception = false;
    tmpPathFixed = false;
    tmpPathFixedConcatenate = false;
    outPath = null;
    finalPath = null;
    dpPath = null;
    tmpPath = null;
    taskTmpPath = null;
    dpCtx = conf.getDpCtx();
    hasDynamicPartitions = conf.hasDynamicPartitions();
    isListBucketingAlterTableConcatenate = conf
        .isListBucketingAlterTableConcatenate();
    listBucketingDepth = conf.getListBucketingDepth();
    Path specPath = conf.getOutputPath();
    updatePaths(Utilities.toTempPath(specPath),
        Utilities.toTaskTempPath(specPath));
    try {
      fs = specPath.getFileSystem(hconf);
      autoDelete = fs.deleteOnExit(outPath);
    } catch (IOException e) {
      this.exception = true;
      throw new HiveException("Failed to initialize AbstractFileMergeOperator",
          e);
    }
    return result;
  }

  // sets up temp and task temp path
  private void updatePaths(Path tp, Path ttp) {
    String taskId = Utilities.getTaskId(jc);
    tmpPath = tp;
    taskTmpPath = ttp;
    finalPath = new Path(tp, taskId);
    outPath = new Path(ttp, Utilities.toTempPath(taskId));
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
    if (depthDiff <=0) {
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
    Path newTaskTmpPath = new Path(taskTmpPath, newPath);
    if (!fs.exists(newTmpPath)) {
      fs.mkdirs(newTmpPath);
    }
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
        if (tmpPathFixed) {
          checkPartitionsMatch(path);
        } else {
          // We haven't fixed the TMP path for this mapper yet
          int depthDiff = path.depth() - tmpPath.depth();
          fixTmpPath(path, depthDiff);
          tmpPathFixed = true;
        }
      }
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    try {
      if (!exception) {
        FileStatus fss = fs.getFileStatus(outPath);
        if (!fs.rename(outPath, finalPath)) {
          throw new IOException(
              "Unable to rename " + outPath + " to " + finalPath);
        }
        LOG.info("renamed path " + outPath + " to " + finalPath + " . File" +
            " size is "
            + fss.getLen());

        // move any incompatible files to final path
        if (!incompatFileSet.isEmpty()) {
          for (Path incompatFile : incompatFileSet) {
            Path destDir = finalPath.getParent();
            try {
              Utilities.renameOrMoveFiles(fs, incompatFile, destDir);
              LOG.info("Moved incompatible file " + incompatFile + " to " +
                  destDir);
            } catch (HiveException e) {
              LOG.error("Unable to move " + incompatFile + " to " + destDir);
              throw new IOException(e);
            }
          }
        }
      } else {
        if (!autoDelete) {
          fs.delete(outPath, true);
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
      Path backupPath = backupOutputPath(fs, outputDir);
      Utilities
          .mvFileToFinalPath(outputDir, hconf, success, LOG, conf.getDpCtx(),
              null, reporter);
      if (success) {
        LOG.info("jobCloseOp moved merged files to output dir: " + outputDir);
      }
      if (backupPath != null) {
        fs.delete(backupPath, true);
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
}
