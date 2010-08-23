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
import org.apache.hadoop.hive.serde2.SerDeException;
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
    List<String> hbaseColumnFamilies = new ArrayList<String>();
    List<String> hbaseColumnQualifiers = new ArrayList<String>();
    List<byte []> hbaseColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> hbaseColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          hbaseColumnFamiliesBytes, hbaseColumnQualifiers, hbaseColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    if (hbaseColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    Scan scan = new Scan();
    boolean empty = true;

    if (!addAll) {
      for (int i : readColIDs) {
        if (i == iKey) {
          continue;
        }

        if (hbaseColumnQualifiers.get(i) == null) {
          scan.addFamily(hbaseColumnFamiliesBytes.get(i));
        } else {
          scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
        }

        empty = false;
      }
    }

    // The HBase table's row key maps to an Hive table column. In the corner case when only the
    // row key column is selected in Hive, the HBase Scan will be empty i.e. no column family/
    // column qualifier will have been added to the scan. We arbitrarily add at least one column
    // to the HBase scan so that we can retrieve all of the row keys and return them as the Hive
    // tables column projection.
    if (empty) {
      for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
        if (i == iKey) {
          continue;
        }

        if (hbaseColumnQualifiers.get(i) == null) {
          scan.addFamily(hbaseColumnFamiliesBytes.get(i));
        } else {
          scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
        }

        if (!addAll) {
          break;
        }
      }
    }

    setScan(scan);
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

    List<String> hbaseColumnFamilies = new ArrayList<String>();
    List<String> hbaseColumnQualifiers = new ArrayList<String>();
    List<byte []> hbaseColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> hbaseColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          hbaseColumnFamiliesBytes, hbaseColumnQualifiers, hbaseColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }

    Scan scan = new Scan();

    // REVIEW:  are we supposed to be applying the getReadColumnIDs
    // same as in getRecordReader?
    for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
      if (i == iKey) {
        continue;
      }

      if (hbaseColumnQualifiers.get(i) == null) {
        scan.addFamily(hbaseColumnFamiliesBytes.get(i));
      } else {
        scan.addColumn(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i));
      }
    }

    setScan(scan);
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
