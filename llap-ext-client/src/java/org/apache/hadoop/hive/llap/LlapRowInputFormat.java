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

package org.apache.hadoop.hive.llap;

import java.io.IOException;

import org.apache.hadoop.hive.llap.LlapBaseRecordReader;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.hive.llap.LlapRowRecordReader;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.Schema;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class LlapRowInputFormat implements InputFormat<NullWritable, Row> {

  private LlapBaseInputFormat<Text> baseInputFormat = new LlapBaseInputFormat<Text>();

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    return baseInputFormat.getSplits(job, numSplits);
  }

  @Override
  public RecordReader<NullWritable, Row> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    LlapInputSplit llapSplit = (LlapInputSplit) split;
    LlapBaseRecordReader<Text> reader = (LlapBaseRecordReader<Text>) baseInputFormat.getRecordReader(llapSplit, job, reporter);
    return new LlapRowRecordReader(job, reader.getSchema(), reader);
  }
}
