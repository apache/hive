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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * HiveHFileOutputFormat implements HiveOutputFormat for HFile bulk
 * loading.  Until HBASE-1861 is implemented, it can only be used
 * for loading a table with a single column family.
 */
public class HiveHFileOutputFormat extends
    HFileOutputFormat implements
    HiveOutputFormat<ImmutableBytesWritable, KeyValue> {

  private static final String HFILE_FAMILY_PATH = "hfile.family.path";

  static final Log LOG = LogFactory.getLog(
    HiveHFileOutputFormat.class.getName());

  private
  org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable, KeyValue>
  getFileWriter(org.apache.hadoop.mapreduce.TaskAttemptContext tac)
  throws IOException {
    try {
      return super.getRecordWriter(tac);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public RecordWriter getHiveRecordWriter(
    final JobConf jc,
    final Path finalOutPath,
    Class<? extends Writable> valueClass,
    boolean isCompressed,
    Properties tableProperties,
    final Progressable progressable) throws IOException {

    // Read configuration for the target path
    String hfilePath = tableProperties.getProperty(HFILE_FAMILY_PATH);
    if (hfilePath == null) {
      throw new RuntimeException(
        "Please set " + HFILE_FAMILY_PATH + " to target location for HFiles");
    }

    // Target path's last component is also the column family name.
    final Path columnFamilyPath = new Path(hfilePath);
    final String columnFamilyName = columnFamilyPath.getName();
    final byte [] columnFamilyNameBytes = Bytes.toBytes(columnFamilyName);
    final Job job = new Job(jc);
    setCompressOutput(job, isCompressed);
    setOutputPath(job, finalOutPath);

    // Create the HFile writer
    final org.apache.hadoop.mapreduce.TaskAttemptContext tac =
      ShimLoader.getHadoopShims().newTaskAttemptContext(
          job.getConfiguration(), progressable);

    final Path outputdir = FileOutputFormat.getOutputPath(tac);
    final org.apache.hadoop.mapreduce.RecordWriter<
      ImmutableBytesWritable, KeyValue> fileWriter = getFileWriter(tac);

    // Individual columns are going to be pivoted to HBase cells,
    // and for each row, they need to be written out in order
    // of column name, so sort the column names now, creating a
    // mapping to their column position.  However, the first
    // column is interpreted as the row key.
    String columnList = tableProperties.getProperty("columns");
    String [] columnArray = columnList.split(",");
    final SortedMap<byte [], Integer> columnMap =
      new TreeMap<byte [], Integer>(Bytes.BYTES_COMPARATOR);
    int i = 0;
    for (String columnName : columnArray) {
      if (i != 0) {
        columnMap.put(Bytes.toBytes(columnName), i);
      }
      ++i;
    }

    return new RecordWriter() {

      @Override
      public void close(boolean abort) throws IOException {
        try {
          fileWriter.close(null);
          if (abort) {
            return;
          }
          // Move the region file(s) from the task output directory
          // to the location specified by the user.  There should
          // actually only be one (each reducer produces one HFile),
          // but we don't know what its name is.
          FileSystem fs = outputdir.getFileSystem(jc);
          fs.mkdirs(columnFamilyPath);
          Path srcDir = outputdir;
          for (;;) {
            FileStatus [] files = fs.listStatus(srcDir);
            if ((files == null) || (files.length == 0)) {
              throw new IOException("No files found in " + srcDir);
            }
            if (files.length != 1) {
              throw new IOException("Multiple files found in " + srcDir);
            }
            srcDir = files[0].getPath();
            if (srcDir.getName().equals(columnFamilyName)) {
              break;
            }
          }
          for (FileStatus regionFile : fs.listStatus(srcDir)) {
            fs.rename(
              regionFile.getPath(),
              new Path(
                columnFamilyPath,
                regionFile.getPath().getName()));
          }
          // Hive actually wants a file as task output (not a directory), so
          // replace the empty directory with an empty file to keep it happy.
          fs.delete(outputdir, true);
          fs.createNewFile(outputdir);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        // Decompose the incoming text row into fields.
        String s = ((Text) w).toString();
        String [] fields = s.split("\u0001");
        assert(fields.length <= (columnMap.size() + 1));
        // First field is the row key.
        byte [] rowKeyBytes = Bytes.toBytes(fields[0]);
        // Remaining fields are cells addressed by column name within row.
        for (Map.Entry<byte [], Integer> entry : columnMap.entrySet()) {
          byte [] columnNameBytes = entry.getKey();
          int iColumn = entry.getValue();
          String val;
          if (iColumn >= fields.length) {
            // trailing blank field
            val = "";
          } else {
            val = fields[iColumn];
            if ("\\N".equals(val)) {
              // omit nulls
              continue;
            }
          }
          byte [] valBytes = Bytes.toBytes(val);
          KeyValue kv = new KeyValue(
            rowKeyBytes,
            columnFamilyNameBytes,
            columnNameBytes,
            valBytes);
          try {
            fileWriter.write(null, kv);
          } catch (InterruptedException ex) {
            throw new IOException(ex);
          }
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException {
    //delegate to the new api
    Job job = new Job(jc);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

    checkOutputSpecs(jobContext);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    throw new NotImplementedException("This will not be invoked");
  }
}
