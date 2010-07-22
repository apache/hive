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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning.
 */
public class HiveHBaseTableInputFormat extends TableInputFormatBase
    implements InputFormat<ImmutableBytesWritable, Result> {

  static final Log LOG = LogFactory.getLog(HiveHBaseTableInputFormat.class);

  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
    InputSplit split,
    JobConf jobConf,
    final Reporter reporter) throws IOException {

    HBaseSplit hbaseSplit = (HBaseSplit) split;
    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    setHTable(new HTable(new HBaseConfiguration(jobConf), Bytes.toBytes(hbaseTableName)));

    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    List<String> columns = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping);
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    if (columns.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    List<byte []> scanColumns = new ArrayList<byte []>();
    boolean addAll = (readColIDs.size() == 0);
    if (!addAll) {
      for (int iColumn : readColIDs) {
        String column = columns.get(iColumn);
        if (HBaseSerDe.isSpecialColumn(column)) {
          continue;
        }
        scanColumns.add(Bytes.toBytes(column));
      }
    }
    if (scanColumns.isEmpty()) {
      for (String column : columns) {
        if (HBaseSerDe.isSpecialColumn(column)) {
          continue;
        }
        scanColumns.add(Bytes.toBytes(column));
        if (!addAll) {
          break;
        }
      }
    }

    setScan(new Scan().addColumns(scanColumns.toArray(new byte[0][])));
    org.apache.hadoop.hbase.mapreduce.TableSplit tableSplit = hbaseSplit.getSplit();

    Job job = new Job(jobConf);
    TaskAttemptContext tac =
      new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {

        @Override
        public void progress() {
          reporter.progress();
        }
      };

    final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, Result>
    recordReader = createRecordReader(tableSplit, tac);

    return new RecordReader<ImmutableBytesWritable, Result>() {

      @Override
      public void close() throws IOException {
        recordReader.close();
      }

      @Override
      public ImmutableBytesWritable createKey() {
        return new ImmutableBytesWritable();
      }

      @Override
      public Result createValue() {
        return new Result();
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public float getProgress() throws IOException {
        float progress = 0.0F;

        try {
          progress = recordReader.getProgress();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }

        return progress;
      }

      @Override
      public boolean next(ImmutableBytesWritable rowKey, Result value) throws IOException {

        boolean next = false;

        try {
          next = recordReader.nextKeyValue();

          if (next) {
            rowKey.set(recordReader.getCurrentValue().getRow());
            Writables.copyWritable(recordReader.getCurrentValue(), value);
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        }

        return next;
      }
    };
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {

    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    setHTable(new HTable(new HBaseConfiguration(jobConf), Bytes.toBytes(hbaseTableName)));
    String hbaseColumnsMapping = jobConf.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    if (hbaseColumnsMapping == null) {
      throw new IOException("hbase.columns.mapping required for HBase Table.");
    }

    // REVIEW:  are we supposed to be applying the getReadColumnIDs
    // same as in getRecordReader?
    List<String> columns = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping);
    List<byte []> inputColumns = new ArrayList<byte []>();
    for (String column : columns) {
      if (HBaseSerDe.isSpecialColumn(column)) {
        continue;
      }
      inputColumns.add(Bytes.toBytes(column));
    }

    setScan(new Scan().addColumns(inputColumns.toArray(new byte[0][])));
    Job job = new Job(jobConf);
    JobContext jobContext = new JobContext(job.getConfiguration(), job.getJobID());
    Path [] tablePaths = FileInputFormat.getInputPaths(jobContext);
    List<org.apache.hadoop.mapreduce.InputSplit> splits = getSplits(jobContext);
    InputSplit [] results = new InputSplit[splits.size()];

    for (int i = 0; i < splits.size(); i++) {
      results[i] = new HBaseSplit((TableSplit) splits.get(i), tablePaths[0]);
    }

    return results;
  }
}
