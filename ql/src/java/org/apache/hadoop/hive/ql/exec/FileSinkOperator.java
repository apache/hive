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

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

/**
 * File Sink operator implementation.
 **/
public class FileSinkOperator extends TerminalOperator<FileSinkDesc> implements
    Serializable {

  /**
   * RecordWriter.
   *
   */
  public static interface RecordWriter {
    void write(Writable w) throws IOException;

    void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  protected transient RecordWriter outWriter;
  protected transient FileSystem fs;
  protected transient Path outPath;
  protected transient Path finalPath;
  protected transient Serializer serializer;
  protected transient BytesWritable commonKey = new BytesWritable();
  protected transient TableIdEnum tabIdEnum = null;
  private transient LongWritable row_count;

  /**
   * TableIdEnum.
   *
   */
  public static enum TableIdEnum {
    TABLE_ID_1_ROWCOUNT,
    TABLE_ID_2_ROWCOUNT,
    TABLE_ID_3_ROWCOUNT,
    TABLE_ID_4_ROWCOUNT,
    TABLE_ID_5_ROWCOUNT,
    TABLE_ID_6_ROWCOUNT,
    TABLE_ID_7_ROWCOUNT,
    TABLE_ID_8_ROWCOUNT,
    TABLE_ID_9_ROWCOUNT,
    TABLE_ID_10_ROWCOUNT,
    TABLE_ID_11_ROWCOUNT,
    TABLE_ID_12_ROWCOUNT,
    TABLE_ID_13_ROWCOUNT,
    TABLE_ID_14_ROWCOUNT,
    TABLE_ID_15_ROWCOUNT;
  }

  protected transient boolean autoDelete = false;

  private void commit() throws IOException {
    if (!fs.rename(outPath, finalPath)) {
      throw new IOException("Unable to rename output to: " + finalPath);
    }
    LOG.info("Committed to output file: " + finalPath);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      serializer = (Serializer) conf.getTableInfo().getDeserializerClass()
          .newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());

      JobConf jc;
      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        // test code path
        jc = new JobConf(hconf, ExecDriver.class);
      }

      int id = conf.getDestTableId();
      if ((id != 0) && (id <= TableIdEnum.values().length)) {
        String enumName = "TABLE_ID_" + String.valueOf(id) + "_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);

      }
      String specPath = conf.getDirName();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId = Utilities.getTaskId(hconf);
      fs = (new Path(specPath)).getFileSystem(hconf);
      finalPath = new Path(tmpPath, taskId);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));

      LOG.info("Writing to temp file: FS " + outPath);

      HiveOutputFormat<?, ?> hiveOutputFormat = conf.getTableInfo()
          .getOutputFileFormatClass().newInstance();
      boolean isCompressed = conf.getCompressed();

      // The reason to keep these instead of using
      // OutputFormat.getRecordWriter() is that
      // getRecordWriter does not give us enough control over the file name that
      // we create.
      Path parent = Utilities.toTempPath(specPath);
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(parent, jc,
          hiveOutputFormat, isCompressed, finalPath);
      final Class<? extends Writable> outputClass = serializer
          .getSerializedClass();
      outWriter = HiveFileFormatUtils.getHiveRecordWriter(jc, conf
          .getTableInfo(), outputClass, conf, outPath);

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          outPath);

      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  Writable recordValue;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    // Since File Sink is a terminal operator, forward is not called - so,
    // maintain the number of output rows explicitly
    if (counterNameToEnum != null) {
      ++outputRows;
      if (outputRows % 1000 == 0) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }

    try {
      if (reporter != null) {
        reporter.progress();
      }
      // user SerDe to serialize r, and write it out
      recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      outWriter.write(recordValue);
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    if (!abort) {
      if (outWriter != null) {
        try {
          outWriter.close(abort);
          commit();
        } catch (IOException e) {
          throw new HiveException(e);
        }
      }
    } else {
      // Will come here if an Exception was thrown in map() or reduce().
      // Hadoop always call close() even if an Exception was thrown in map() or
      // reduce().
      try {
        outWriter.close(abort);
        if (!autoDelete) {
          fs.delete(outPath, true);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return new String("FS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success) throws HiveException {
    try {
      if (conf != null) {
        String specPath = conf.getDirName();
        FileSinkOperator.mvFileToFinalPath(specPath, hconf, success, LOG);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success);
  }

  public static void mvFileToFinalPath(String specPath, Configuration hconf,
      boolean success, Log log) throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    if (success) {
      if (fs.exists(tmpPath)) {
        // Step1: rename tmp output folder to intermediate path. After this
        // point, updates from speculative tasks still writing to tmpPath
        // will not appear in finalPath.
        log.info("Moving tmp dir: " + tmpPath + " to: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        // Step2: remove any tmp file or double-committed output files
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        // Step3: move to the file destination
        log.info("Moving tmp dir: " + intermediatePath + " to: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }

  @Override
  public int getType() {
    return OperatorType.FILESINK;
  }
}
