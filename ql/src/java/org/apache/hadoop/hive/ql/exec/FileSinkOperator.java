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

import java.io.*;
import java.lang.reflect.Method;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.exec.FilterOperator.Counter;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

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
    if(!fs.rename(outPath, finalPath)) {
      throw new IOException ("Unable to rename output to: " + finalPath);
    }
    LOG.info("Committed to output file: " + finalPath);
  }

  public void close(boolean abort) throws HiveException {
    if(!abort) {
      if (outWriter != null) {
        try {
          outWriter.close(abort);
          commit();
        } catch (IOException e) {
          throw new HiveException("Error in committing output in file: "+ outPath.toString());
        }
      }
    } else {
      try {
        outWriter.close(abort);
        if(!autoDelete)
          fs.delete(outPath, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void initialize(Configuration hconf, Reporter reporter) throws HiveException {
    super.initialize(hconf, reporter);
    
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
      fs = FileSystem.get(hconf);
      finalPath = new Path(Utilities.toTempPath(conf.getDirName()), Utilities.getTaskId(hconf));
      outPath = new Path(Utilities.toTempPath(conf.getDirName()), Utilities.toTempPath(Utilities.getTaskId(hconf)));

      LOG.info("Writing to temp file: " + outPath);

      OutputFormat<?, ?> outputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
      final Class<? extends Writable> outputClass = serializer.getSerializedClass();
      boolean isCompressed = conf.getCompressed();

      // The reason to keep these instead of using OutputFormat.getRecordWriter() is that
      // getRecordWriter does not give us enough control over the file name that we create.
      if(outputFormat instanceof IgnoreKeyTextOutputFormat) {
        finalPath = new Path(Utilities.toTempPath(conf.getDirName()), Utilities.getTaskId(hconf) +
                             Utilities.getFileExtension(jc, isCompressed));

        String rowSeparatorString = conf.getTableInfo().getProperties().getProperty(Constants.LINE_DELIM, "\n");
        int rowSeparator = 0;
        try {
          rowSeparator = Byte.parseByte(rowSeparatorString); 
        } catch (NumberFormatException e) {
          rowSeparator = rowSeparatorString.charAt(0); 
        }
        final int finalRowSeparator = rowSeparator;  
        final OutputStream outStream = Utilities.createCompressedStream(jc, fs.create(outPath), isCompressed);
        outWriter = new RecordWriter () {
            public void write(Writable r) throws IOException {
              if (r instanceof Text) {
                Text tr = (Text)r;
                outStream.write(tr.getBytes(), 0, tr.getLength());
                outStream.write(finalRowSeparator);
              } else {
                // DynamicSerDe always writes out BytesWritable
                BytesWritable bw = (BytesWritable)r;
                outStream.write(bw.get(), 0, bw.getSize());
                outStream.write(finalRowSeparator);
              }
            }
            public void close(boolean abort) throws IOException {
              outStream.close();
            }
          };
      } else if (outputFormat instanceof SequenceFileOutputFormat) {
        final SequenceFile.Writer outStream =
          Utilities.createSequenceWriter(jc, fs, outPath, BytesWritable.class, outputClass,
                                         isCompressed);
        outWriter = new RecordWriter () {
            public void write(Writable r) throws IOException {
              outStream.append(commonKey, r);
            }
            public void close(boolean abort) throws IOException {
              outStream.close();
            }
          };
      } else {
        // should never come here - we should be catching this in ddl command
        throw new HiveException ("Illegal outputformat: " + outputFormat.getClass().getName());
      }

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      try {
        Method deleteOnExit = FileSystem.class.getDeclaredMethod("deleteOnExit", new Class [] {Path.class});
        deleteOnExit.setAccessible(true);
        deleteOnExit.invoke(fs, outPath);
        autoDelete = true;
      } catch (Exception e) {}

    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  Writable recordValue; 
  public void process(Object row, ObjectInspector rowInspector) throws HiveException {
    try {
      reporter.progress();
      // user SerDe to serialize r, and write it out
      recordValue = serializer.serialize(row, rowInspector);
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
        fs = FileSystem.get(hconf);
        Path tmpPath = Utilities.toTempPath(conf.getDirName());
        Path finalPath = new Path(conf.getDirName());
        if(success) {
          if(fs.exists(tmpPath)) {
            // Step1: rename tmp output folder to final path. After this point, 
            // updates from speculative tasks still writing to tmpPath will not 
            // appear in finalPath
            LOG.info("Renaming tmp dir: " + tmpPath + " to: " + finalPath);
            if(!fs.rename(tmpPath, finalPath)) {
              throw new HiveException("Unable to commit result directory: " + finalPath);
            }
            // Step2: Clean any temp files from finalPath
            Utilities.removeTempFiles(fs, finalPath);
          }
        } else {
          fs.delete(tmpPath);
        }
      }
    } catch (IOException e) {
      throw new HiveException (e);
    }
    super.jobClose(hconf, success);
  }
}
