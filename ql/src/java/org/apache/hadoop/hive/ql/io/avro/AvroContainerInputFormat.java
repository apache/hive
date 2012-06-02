/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.avro;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroContainerInputFormat
        extends FileInputFormat<NullWritable, AvroGenericRecordWritable> implements JobConfigurable {
  protected JobConf jobConf;

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
    for (FileStatus file : super.listStatus(job)) {
      result.add(file);
    }
    return result.toArray(new FileStatus[0]);
  }

  @Override
  public RecordReader<NullWritable, AvroGenericRecordWritable>
    getRecordReader(InputSplit inputSplit, JobConf jc, Reporter reporter) throws IOException {
    return new AvroGenericRecordReader(jc, (FileSplit) inputSplit, reporter);
  }

  @Override
  public void configure(JobConf jobConf) {
    this.jobConf = jobConf;
  }
}
