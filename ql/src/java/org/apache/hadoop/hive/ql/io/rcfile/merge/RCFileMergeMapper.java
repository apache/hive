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

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class RCFileMergeMapper extends MapReduceBase implements
    Mapper<Object, RCFileValueBufferWrapper, Object, Object> {

  private JobConf jc;
  Class<? extends Writable> outputClass;
  RCFile.Writer outWriter;

  Path finalPath;
  FileSystem fs;

  boolean exception = false;
  boolean autoDelete = false;
  Path outPath;

  CompressionCodec codec = null;
  int columnNumber = 0;

  boolean hasDynamicPartitions = false;
  boolean tmpPathFixed = false;
  Path tmpPath;
  Path taskTmpPath;
  Path dpPath;

  public final static Log LOG = LogFactory.getLog("RCFileMergeMapper");

  public RCFileMergeMapper() {
  }

  @Override
  public void configure(JobConf job) {
    jc = job;
    hasDynamicPartitions = HiveConf.getBoolVar(job,
        HiveConf.ConfVars.HIVEMERGECURRENTJOBHASDYNAMICPARTITIONS);

    String specPath = RCFileBlockMergeOutputFormat.getMergeOutputPath(job)
        .toString();
    Path tmpPath = Utilities.toTempPath(specPath);
    Path taskTmpPath = Utilities.toTaskTempPath(specPath);
    updatePaths(tmpPath, taskTmpPath);
    try {
      fs = (new Path(specPath)).getFileSystem(job);
      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          outPath);
    } catch (IOException e) {
      this.exception = true;
      throw new RuntimeException(e);
    }
  }

  private void updatePaths(Path tmpPath, Path taskTmpPath) {
    String taskId = Utilities.getTaskId(jc);
    this.tmpPath = tmpPath;
    this.taskTmpPath = taskTmpPath;
    finalPath = new Path(tmpPath, taskId);
    outPath = new Path(taskTmpPath, Utilities.toTempPath(taskId));
  }

  @Override
  public void map(Object k, RCFileValueBufferWrapper value,
      OutputCollector<Object, Object> output, Reporter reporter)
      throws IOException {
    try {

      RCFileKeyBufferWrapper key = null;
      if (k instanceof CombineHiveKey) {
        key = (RCFileKeyBufferWrapper) ((CombineHiveKey) k).getKey();
      } else {
        key = (RCFileKeyBufferWrapper) k;
      }

      if (hasDynamicPartitions) {
        if (tmpPathFixed) {
          checkPartitionsMatch(key.inputPath.getParent());
        } else {
          // We haven't fixed the TMP path for this mapper yet
          fixTmpPath(key.inputPath.getParent());
          tmpPathFixed = true;
        }
      }

      if (outWriter == null) {
        codec = key.codec;
        columnNumber = key.keyBuffer.getColumnNumber();
        jc.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNumber);
        outWriter = new RCFile.Writer(fs, jc, outPath, null, codec);
      }

      boolean sameCodec = ((codec == key.codec) || codec.getClass().equals(
          key.codec.getClass()));

      if ((key.keyBuffer.getColumnNumber() != columnNumber) || (!sameCodec)) {
        throw new IOException(
            "RCFileMerge failed because the input files use different CompressionCodec or have different column number setting.");
      }

      outWriter.flushBlock(key.keyBuffer, value.valueBuffer, key.recordLength,
          key.keyLength, key.compressedKeyLength);
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }

  /**
   * Validates that each input path belongs to the same partition
   * since each mapper merges the input to a single output directory
   *
   * @param inputPath
   * @throws HiveException
   */
  private void checkPartitionsMatch(Path inputPath) throws HiveException {
    if (!dpPath.equals(inputPath)) {
      // Temp partition input path does not match exist temp path
      String msg = "Multiple partitions for one block merge mapper: " +
          dpPath + " NOT EQUAL TO " + inputPath;
      LOG.error(msg);
      throw new HiveException(msg);
    }
  }

  /**
   * Fixes tmpPath to point to the correct partition.
   * Before this is called, tmpPath will default to the root tmp table dir
   *
   * @param inputPath
   * @throws HiveException
   * @throws IOException
   */
  private void fixTmpPath(Path inputPath)
      throws HiveException, IOException {
    dpPath = inputPath;
    Path newPath = new Path(".");
    int inputDepth = inputPath.depth();
    int tmpDepth = tmpPath.depth();

    // Build the path from bottom up
    while (inputPath != null && inputPath.depth() > tmpDepth) {
      newPath = new Path(inputPath.getName(), newPath);
      inputDepth--;
      inputPath = inputPath.getParent();
    }

    Path newTmpPath = new Path(tmpPath, newPath);
    Path newTaskTmpPath = new Path(taskTmpPath, newPath);
    if (!fs.exists(newTmpPath)) {
      fs.mkdirs(newTmpPath);
    }
    updatePaths(newTmpPath, newTaskTmpPath);
  }

  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter == null) {
      return;
    }

    outWriter.close();
    outWriter = null;

    if (!exception) {
      FileStatus fss = fs.getFileStatus(outPath);
      LOG.info("renamed path " + outPath + " to " + finalPath
          + " . File size is " + fss.getLen());
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to " + finalPath);
      }
    } else {
      if (!autoDelete) {
        fs.delete(outPath, true);
      }
    }
  }

  public static String BACKUP_PREFIX = "_backup.";

  public static Path backupOutputPath(FileSystem fs, Path outpath, JobConf job)
      throws IOException, HiveException {
    if (fs.exists(outpath)) {
      Path backupPath = new Path(outpath.getParent(), BACKUP_PREFIX
          + outpath.getName());
      Utilities.rename(fs, outpath, backupPath);
      return backupPath;
    } else {
      return null;
    }
  }

  public static void jobClose(String outputPath, boolean success, JobConf job,
      LogHelper console, DynamicPartitionCtx dynPartCtx) throws HiveException, IOException {
    Path outpath = new Path(outputPath);
    FileSystem fs = outpath.getFileSystem(job);
    Path backupPath = backupOutputPath(fs, outpath, job);
    Utilities.mvFileToFinalPath(outputPath, job, success, LOG, dynPartCtx, null);
    fs.delete(backupPath, true);
  }

}
