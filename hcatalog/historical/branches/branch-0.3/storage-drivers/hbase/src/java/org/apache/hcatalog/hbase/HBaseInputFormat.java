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

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.mapreduce.InputJobInfo;

/**
 * This class HBaseInputFormat is a wrapper class of TableInputFormat in HBase.
 */
class HBaseInputFormat extends InputFormat<ImmutableBytesWritable, Result> implements Configurable{

    private final TableInputFormat inputFormat;
    private final InputJobInfo jobInfo;
    private Configuration conf;

    public HBaseInputFormat(InputJobInfo jobInfo) {
        inputFormat = new TableInputFormat();
        this.jobInfo = jobInfo;
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
    public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
            InputSplit split, TaskAttemptContext tac) throws IOException,
            InterruptedException {

          String tableName = inputFormat.getConf().get(TableInputFormat.INPUT_TABLE);
          TableSplit tSplit = (TableSplit) split;
          HbaseSnapshotRecordReader recordReader = new HbaseSnapshotRecordReader(jobInfo);
          Scan sc = new Scan(inputFormat.getScan());
          sc.setStartRow(tSplit.getStartRow());
          sc.setStopRow(tSplit.getEndRow());
          recordReader.setScan(sc);
          recordReader.setHTable(new HTable(this.conf, tableName));
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
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {

        String tableName = this.conf.get(TableInputFormat.INPUT_TABLE);
        if (tableName == null) {
           throw new IOException("The input table is not set. The input splits cannot be created.");
        }
        return inputFormat.getSplits(jobContext);
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        inputFormat.setConf(conf);
    }

    public Scan getScan() {
        return inputFormat.getScan();
    }

    public void setScan(Scan scan) {
        inputFormat.setScan(scan);
    }

    /* @return
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
       return this.conf;
    }

}
