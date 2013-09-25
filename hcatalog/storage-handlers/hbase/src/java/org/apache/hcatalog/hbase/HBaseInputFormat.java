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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatMapRedUtil;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.mapreduce.InputJobInfo;

/**
 * This class HBaseInputFormat is a wrapper class of TableInputFormat in HBase.
 */
class HBaseInputFormat implements InputFormat<ImmutableBytesWritable, Result> {

  private final TableInputFormat inputFormat;

  public HBaseInputFormat() {
    inputFormat = new TableInputFormat();
  }

  /*
   * @param instance of InputSplit
   *
   * @param instance of TaskAttemptContext
   *
   * @return RecordReader
   *
   * @throws IOException
   *
   * @throws InterruptedException
   *
   * @see
   * org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache
   * .hadoop.mapreduce.InputSplit,
   * org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
    InputSplit split, JobConf job, Reporter reporter)
    throws IOException {
    String jobString = job.get(HCatConstants.HCAT_KEY_JOB_INFO);
    InputJobInfo inputJobInfo = (InputJobInfo) HCatUtil.deserialize(jobString);

    String tableName = job.get(TableInputFormat.INPUT_TABLE);
    TableSplit tSplit = (TableSplit) split;
    HbaseSnapshotRecordReader recordReader = new HbaseSnapshotRecordReader(inputJobInfo, job);
    inputFormat.setConf(job);
    Scan inputScan = inputFormat.getScan();
    // TODO: Make the caching configurable by the user
    inputScan.setCaching(200);
    inputScan.setCacheBlocks(false);
    Scan sc = new Scan(inputScan);
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    recordReader.setScan(sc);
    recordReader.setHTable(new HTable(job, tableName));
    recordReader.init();
    return recordReader;
  }

  /*
   * @param jobContext
   *
   * @return List of InputSplit
   *
   * @throws IOException
   *
   * @throws InterruptedException
   *
   * @see
   * org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce
   * .JobContext)
   */
  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    inputFormat.setConf(job);
    return convertSplits(inputFormat.getSplits(HCatMapRedUtil.createJobContext(job, null,
      Reporter.NULL)));
  }

  private InputSplit[] convertSplits(List<org.apache.hadoop.mapreduce.InputSplit> splits) {
    InputSplit[] converted = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); i++) {
      org.apache.hadoop.hbase.mapreduce.TableSplit tableSplit =
        (org.apache.hadoop.hbase.mapreduce.TableSplit) splits.get(i);
      TableSplit newTableSplit = new TableSplit(tableSplit.getTableName(),
        tableSplit.getStartRow(),
        tableSplit.getEndRow(), tableSplit.getRegionLocation());
      converted[i] = newTableSplit;
    }
    return converted;
  }

}
