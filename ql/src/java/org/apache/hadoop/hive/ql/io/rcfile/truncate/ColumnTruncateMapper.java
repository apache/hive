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

package org.apache.hadoop.hive.ql.io.rcfile.truncate;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileKeyBufferWrapper;
import org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileValueBufferWrapper;
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
public class ColumnTruncateMapper extends MapReduceBase implements
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

  boolean tmpPathFixedConcatenate = false;
  boolean tmpPathFixed = false;
  Path tmpPath;
  Path taskTmpPath;
  Path dpPath;
  ColumnTruncateWork work;

  public final static Log LOG = LogFactory.getLog(ColumnTruncateMapper.class.getName());

  public ColumnTruncateMapper() {
  }

  @Override
  public void configure(JobConf job) {
    jc = job;
    work = (ColumnTruncateWork) Utilities.getMapWork(job);

    String specPath = work.getOutputDir();
    Path tmpPath = Utilities.toTempPath(specPath);
    Path taskTmpPath = Utilities.toTaskTempPath(specPath);
    updatePaths(tmpPath, taskTmpPath);
    try {
      fs = (new Path(specPath)).getFileSystem(job);
      autoDelete = fs.deleteOnExit(outPath);
    } catch (IOException e) {
      this.exception = true;
      throw new RuntimeException(e);
    }
  }

  private void updatePaths(Path tmpPath, Path taskTmpPath) {
    String taskId = Utilities.getTaskId(jc);
    this.tmpPath = tmpPath;
    this.taskTmpPath = taskTmpPath;
    String inputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);
    int lastSeparator = inputFile.lastIndexOf(Path.SEPARATOR) + 1;
    finalPath = new Path(tmpPath, inputFile.substring(lastSeparator));
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

      if (work.getListBucketingCtx().calculateListBucketingLevel() > 0) {
        if (!this.tmpPathFixedConcatenate) {
          fixTmpPathConcatenate(key.getInputPath().getParent(),
              work.getListBucketingCtx().calculateListBucketingLevel());
          tmpPathFixedConcatenate = true;
        }
      }

      if (outWriter == null) {
        codec = key.getCodec();
        columnNumber = key.getKeyBuffer().getColumnNumber();
        jc.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNumber);
        outWriter = new RCFile.Writer(fs, jc, outPath, null, codec);
      }

      for (Integer i : work.getDroppedColumns()) {
        key.getKeyBuffer().nullColumn(i);
        value.getValueBuffer().nullColumn(i);
      }

      int keyLength = key.getKeyBuffer().getSize();
      int recordLength = key.getKeyBuffer().getSize();
      for (int columnLen : key.getKeyBuffer().getEachColumnValueLen()) {
        recordLength += columnLen;
      }

      outWriter.flushBlock(key.getKeyBuffer(), value.getValueBuffer(), recordLength,
          keyLength, key.getCompressedKeyLength());
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }

  /**
   * Fixes tmpPath to point to the correct list bucketing sub-directories.
   * Before this is called, tmpPath will default to the root tmp table dir
   * Reason to add a new method instead of changing fixTmpPath()
   * Reason 1: logic has slightly difference
   * fixTmpPath(..) needs 2 variables in order to decide path delta which is in variable newPath.
   * 1. inputPath.depth()
   * 2. tmpPath.depth()
   * fixTmpPathConcatenate needs 2 variables too but one of them is different from fixTmpPath(..)
   * 1. inputPath.depth()
   * 2. listBucketingDepth
   * Reason 2: less risks
   * The existing logic is a little not trivial around map() and fixTmpPath(). In order to ensure
   * minimum impact on existing flow, we try to avoid change on existing code/flow but add new code
   * for new feature.
   *
   * @param inputPath
   * @throws HiveException
   * @throws IOException
   */
  private void fixTmpPathConcatenate(Path inputPath, int listBucketingDepth)
      throws HiveException, IOException {
    dpPath = inputPath;
    Path newPath = new Path(".");

    int depth = listBucketingDepth;
    // Build the path from bottom up. pick up list bucketing subdirectories
    while ((inputPath != null) && (depth > 0)) {
      newPath = new Path(inputPath.getName(), newPath);
      inputPath = inputPath.getParent();
      depth--;
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
      LogHelper console, DynamicPartitionCtx dynPartCtx, Reporter reporter
      ) throws HiveException, IOException {
    Path outpath = new Path(outputPath);
    FileSystem fs = outpath.getFileSystem(job);
    Path backupPath = backupOutputPath(fs, outpath, job);
    Utilities.mvFileToFinalPath(outputPath, job, success, LOG, dynPartCtx, null,
      reporter);
    fs.delete(backupPath, true);
  }

}
