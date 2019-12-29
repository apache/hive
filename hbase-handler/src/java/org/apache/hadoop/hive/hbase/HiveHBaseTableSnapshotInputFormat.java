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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

public class HiveHBaseTableSnapshotInputFormat
    implements InputFormat<ImmutableBytesWritable, ResultWritable> {

  TableSnapshotInputFormat delegate = new TableSnapshotInputFormat();

  private static void setColumns(JobConf job) throws IOException {
    Scan scan = HiveHBaseInputFormatUtil.getScan(job);
    job.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN,
      convertScanToString(scan));
  }

  // TODO: Once HBASE-11163 is completed, use that API, or switch to
  // using mapreduce version of the APIs. rather than mapred
  // Copied from HBase's TableMapreduceUtil since it is not public API
  static String convertScanToString(Scan scan) throws IOException {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.getEncoder().encodeToString(proto.toByteArray());
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    setColumns(job);

    // hive depends on FileSplits, so wrap in HBaseSplit
    Path[] tablePaths = FileInputFormat.getInputPaths(job);

    InputSplit [] results = delegate.getSplits(job, numSplits);
    for (int i = 0; i < results.length; i++) {
      results[i] = new HBaseSplit(results[i], tablePaths[0]);
    }

    return results;
  }

  @Override
  public RecordReader<ImmutableBytesWritable, ResultWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    setColumns(job);
    final RecordReader<ImmutableBytesWritable, Result> rr =
      delegate.getRecordReader(((HBaseSplit) split).getSnapshotSplit(), job, reporter);

    return new RecordReader<ImmutableBytesWritable, ResultWritable>() {
      @Override
      public boolean next(ImmutableBytesWritable key, ResultWritable value) throws IOException {
        return rr.next(key, value.getResult());
      }

      @Override
      public ImmutableBytesWritable createKey() {
        return rr.createKey();
      }

      @Override
      public ResultWritable createValue() {
        return new ResultWritable(rr.createValue());
      }

      @Override
      public long getPos() throws IOException {
        return rr.getPos();
      }

      @Override
      public void close() throws IOException {
        rr.close();
      }

      @Override
      public float getProgress() throws IOException {
        return rr.getProgress();
      }
    };
  }
}
