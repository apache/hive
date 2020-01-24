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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * HiveHFileOutputFormat implements HiveOutputFormat for HFile bulk
 * loading.  Until HBASE-1861 is implemented, it can only be used
 * for loading a table with a single column family.
 */
public class HiveHFileOutputFormat extends
    HFileOutputFormat2 implements
    HiveOutputFormat<ImmutableBytesWritable, Cell> {

  public static final String HFILE_FAMILY_PATH = "hfile.family.path";
  public static final String OUTPUT_TABLE_NAME_CONF_KEY =
                      "hbase.mapreduce.hfileoutputformat.table.name";
  static final Logger LOG = LoggerFactory.getLogger(HiveHFileOutputFormat.class.getName());

  private
  org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable, Cell>
  getFileWriter(org.apache.hadoop.mapreduce.TaskAttemptContext tac)
  throws IOException {
    try {
      return super.getRecordWriter(tac);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  /**
   * Retrieve the family path, first check the JobConf, then the table properties.
   * @return the family path or null if not specified.
   */
  public static String getFamilyPath(Configuration jc, Properties tableProps) {
    return jc.get(HFILE_FAMILY_PATH, tableProps.getProperty(HFILE_FAMILY_PATH));
  }

  @Override
  public RecordWriter getHiveRecordWriter(
    final JobConf jc,
    final Path finalOutPath,
    Class<? extends Writable> valueClass,
    boolean isCompressed,
    Properties tableProperties,
    final Progressable progressable) throws IOException {

    String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
    if (hbaseTableName == null) {
      hbaseTableName = tableProperties.getProperty(hive_metastoreConstants.META_TABLE_NAME);
      hbaseTableName = hbaseTableName.toLowerCase();
      if (hbaseTableName.startsWith(HBaseStorageHandler.DEFAULT_PREFIX)) {
        hbaseTableName = hbaseTableName.substring(HBaseStorageHandler.DEFAULT_PREFIX.length());
      }
    }
    jc.set(OUTPUT_TABLE_NAME_CONF_KEY, hbaseTableName);

    // Read configuration for the target path, first from jobconf, then from table properties
    String hfilePath = getFamilyPath(jc, tableProperties);
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
    final Path taskAttemptOutputdir = new FileOutputCommitter(outputdir, tac).getWorkPath();
    final org.apache.hadoop.mapreduce.RecordWriter<
      ImmutableBytesWritable, Cell> fileWriter = getFileWriter(tac);

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
          // Move the hfiles file(s) from the task output directory to the
          // location specified by the user.
          FileSystem fs = outputdir.getFileSystem(jc);
          fs.mkdirs(columnFamilyPath);
          Path srcDir = taskAttemptOutputdir;
          for (;;) {
            FileStatus [] files = fs.listStatus(srcDir, FileUtils.STAGING_DIR_PATH_FILTER);
            if ((files == null) || (files.length == 0)) {
              throw new IOException("No family directories found in " + srcDir);
            }
            if (files.length != 1) {
              throw new IOException("Multiple family directories found in " + srcDir);
            }
            srcDir = files[0].getPath();
            if (srcDir.getName().equals(columnFamilyName)) {
              break;
            }
            if (files[0].isFile()) {
              throw new IOException("No family directories found in " + taskAttemptOutputdir + ". "
                  + "The last component in hfile path should match column family name "
                  + columnFamilyName);
            }
          }
          for (FileStatus regionFile : fs.listStatus(srcDir, FileUtils.STAGING_DIR_PATH_FILTER)) {
            fs.rename(
              regionFile.getPath(),
              new Path(
                columnFamilyPath,
                regionFile.getPath().getName()));
          }
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }

      private void writeText(Text text) throws IOException {
        // Decompose the incoming text row into fields.
        String s = text.toString();
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
          } catch (IOException e) {
            LOG.error("Failed while writing row: " + s);
            throw e;
          } catch (InterruptedException ex) {
            throw new IOException(ex);
          }
        }
      }

      private void writePut(PutWritable put) throws IOException {
        ImmutableBytesWritable row = new ImmutableBytesWritable(put.getPut().getRow());
        SortedMap<byte[], List<Cell>> cells = put.getPut().getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry : cells.entrySet()) {
          Collections.sort(entry.getValue(), new CellComparatorImpl());
          for (Cell c : entry.getValue()) {
            try {
              fileWriter.write(row, KeyValueUtil.copyToNewKeyValue(c));
            } catch (InterruptedException e) {
              throw (InterruptedIOException) new InterruptedIOException().initCause(e);
            }
          }
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        if (w instanceof Text) {
          writeText((Text) w);
        } else if (w instanceof PutWritable) {
          writePut((PutWritable) w);
        } else {
          throw new IOException("Unexpected writable " + w);
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
  public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Cell> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    throw new NotImplementedException("This will not be invoked");
  }
}
