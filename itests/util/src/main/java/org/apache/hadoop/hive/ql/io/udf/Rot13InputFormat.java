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

package org.apache.hadoop.hive.ql.io.udf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

/**
A simple input format that does a rot13 on the inputs
 */
class Rot13InputFormat extends TextInputFormat {

  public static void rot13(byte[] bytes, int offset, int length) {
    for(int i=offset; i < offset+length; i++) {
      if (bytes[i] >= 'A' && bytes[i] <= 'Z') {
        bytes[i] = (byte) ('A' + (bytes[i] - 'A' + 13) % 26);
      } else if (bytes[i] >= 'a' && bytes[i] <= 'z') {
        bytes[i] = (byte) ('a' + (bytes[i] - 'a' + 13) % 26);
      }
    }
  }

  private static class Rot13LineRecordReader extends LineRecordReader {
    Rot13LineRecordReader(JobConf job, FileSplit split) throws IOException {
      super(job, split);
    }

    public synchronized boolean next(LongWritable key,
                                     Text value) throws IOException {
      boolean result = super.next(key, value);
      if (result) {
        System.out.println("Read " + value);
        rot13(value.getBytes(), 0, value.getLength());
        System.out.println("Returned " + value);
      }
      return result;
    }
  }

  public RecordReader<LongWritable, Text>
    getRecordReader(InputSplit genericSplit, JobConf job,
                    Reporter reporter) throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new Rot13LineRecordReader(job, (FileSplit) genericSplit);
  }
}
