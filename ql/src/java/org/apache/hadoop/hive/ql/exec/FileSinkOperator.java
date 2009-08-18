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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.LzoCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * File Sink operator implementation
 **/
public class FileSinkOperator extends TerminalOperator <fileSinkDesc> implements Serializable {

  public static interface RecordWriter {
    public void write(Writable w) throws IOException;
    public void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  transient protected RecordWriter outWriter;
  transient protected FileSystem fs;
  transient protected Path outPath;
  transient protected Path finalPath;
  transient protected Serializer serializer;
  transient protected BytesWritable commonKey = new BytesWritable();
  transient protected TableIdEnum tabIdEnum = null;
  transient private  LongWritable row_count;
  public static enum TableIdEnum {

    TABLE_ID_1_ROWCOUNT, TABLE_ID_2_ROWCOUNT, TABLE_ID_3_ROWCOUNT, TABLE_ID_4_ROWCOUNT, TABLE_ID_5_ROWCOUNT, TABLE_ID_6_ROWCOUNT, TABLE_ID_7_ROWCOUNT, TABLE_ID_8_ROWCOUNT, TABLE_ID_9_ROWCOUNT, TABLE_ID_10_ROWCOUNT, TABLE_ID_11_ROWCOUNT, TABLE_ID_12_ROWCOUNT, TABLE_ID_13_ROWCOUNT, TABLE_ID_14_ROWCOUNT, TABLE_ID_15_ROWCOUNT;

  }
  transient protected boolean autoDelete = false;

  private void commit() throws IOException {
   if (!fs.rename(outPath, finalPath)) {
      throw new IOException ("Unable to rename output to: " + finalPath);
    }
    LOG.info("Committed to output file: " + finalPath);
  }

  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      serializer = (Serializer)conf.getTableInfo().getDeserializerClass().newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());
      
  
      JobConf jc;
      if(hconf instanceof JobConf) {
        jc = (JobConf)hconf;
      } else {
        // test code path
        jc = new JobConf(hconf, ExecDriver.class);
      }

      int id = conf.getDestTableId();
      if ((id != 0) && (id <= TableIdEnum.values().length)){
        String enumName = "TABLE_ID_"+String.valueOf(id)+"_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);
        
      }
      String specPath = conf.getDirName();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId =  Utilities.getTaskId(hconf);
      fs =(new Path(specPath)).getFileSystem(hconf);
      finalPath = new Path(tmpPath, taskId);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));

      LOG.info("Writing to temp file: FS " + outPath);

      HiveOutputFormat<?, ?> hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
      final Class<? extends Writable> outputClass = serializer.getSerializedClass();
      boolean isCompressed = conf.getCompressed();

      // The reason to keep these instead of using
      // OutputFormat.getRecordWriter() is that
      // getRecordWriter does not give us enough control over the file name that
      // we create.
      Path parent = Utilities.toTempPath(specPath);
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(parent, jc, hiveOutputFormat, isCompressed, finalPath);
      tableDesc tableInfo = conf.getTableInfo();
      JobConf jc_output = jc;
      if (isCompressed) {
        jc_output = new JobConf(jc);
        String codecStr = conf.getCompressCodec();
        if (codecStr != null && !codecStr.trim().equals("")) {
          Class<? extends CompressionCodec> codec = (Class<? extends CompressionCodec>) Class.forName(codecStr);
          FileOutputFormat.setOutputCompressorClass(jc_output, codec);
        }
        String type = conf.getCompressType();
        if(type !=null && !type.trim().equals("")) {
          CompressionType style = CompressionType.valueOf(type);
          SequenceFileOutputFormat.setOutputCompressionType(jc, style);
        }
      }
      outWriter = getRecordWriter(jc_output, hiveOutputFormat, outputClass, isCompressed, tableInfo.getProperties(), outPath);

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs, outPath);

      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public static RecordWriter getRecordWriter(JobConf jc, HiveOutputFormat<?, ?> hiveOutputFormat,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath) throws IOException, HiveException {
    if (hiveOutputFormat != null) {
      return hiveOutputFormat.getHiveRecordWriter(jc, outPath, valueClass, isCompressed, tableProp, null);
    }
    return null;
  }

  Writable recordValue; 
  public void process(Object row, int tag) throws HiveException {
    try {
      if (reporter != null)
        reporter.progress();
      // user SerDe to serialize r, and write it out
      recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (row_count != null){
        row_count.set(row_count.get()+ 1);
      }
        
      outWriter.write(recordValue);
    } catch (IOException e) {
      throw new HiveException (e);
    } catch (SerDeException e) {
      throw new HiveException (e);
    }
  }

  public void close(boolean abort) throws HiveException {
    if (state == State.CLOSE) 
      return;
  
    state = State.CLOSE;
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
      // Hadoop always call close() even if an Exception was thrown in map() or reduce(). 
      try {
        outWriter.close(abort);
        if(!autoDelete)
          fs.delete(outPath, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return the name of the operator
   */
  public String getName() {
    return new String("FS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success) throws HiveException { 
    try {
      if(conf != null) {
        String specPath = conf.getDirName();
        fs = (new Path(specPath)).getFileSystem(hconf);
        Path tmpPath = Utilities.toTempPath(specPath);
        Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName() + ".intermediate");
        Path finalPath = new Path(specPath);
        if(success) {
          if(fs.exists(tmpPath)) {
            // Step1: rename tmp output folder to intermediate path. After this
            // point, updates from speculative tasks still writing to tmpPath 
            // will not appear in finalPath.
            LOG.info("Moving tmp dir: " + tmpPath + " to: " + intermediatePath);
            Utilities.rename(fs, tmpPath, intermediatePath);
            // Step2: remove any tmp file or double-committed output files
            Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
            // Step3: move to the file destination
            LOG.info("Moving tmp dir: " + intermediatePath + " to: " + finalPath);
            Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
          }
        } else {
          fs.delete(tmpPath, true);
        }
      }
    } catch (IOException e) {
      throw new HiveException (e);
    }
    super.jobClose(hconf, success);
  }
  
}
