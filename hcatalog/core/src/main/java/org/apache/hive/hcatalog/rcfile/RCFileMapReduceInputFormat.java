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
package org.apache.hive.hcatalog.rcfile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class RCFileMapReduceInputFormat<K extends LongWritable, V extends BytesRefArrayWritable>
  extends FileInputFormat<LongWritable, BytesRefArrayWritable> {

  @Override
  public RecordReader<LongWritable, BytesRefArrayWritable> createRecordReader(InputSplit split,
                                        TaskAttemptContext context) throws IOException, InterruptedException {

    context.setStatus(split.toString());
    return new RCFileMapReduceRecordReader<LongWritable, BytesRefArrayWritable>();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    job.getConfiguration().setLong("mapred.min.split.size", SequenceFile.SYNC_INTERVAL);
    return super.getSplits(job);
  }
}
