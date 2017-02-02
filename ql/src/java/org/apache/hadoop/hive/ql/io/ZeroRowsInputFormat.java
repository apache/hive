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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat.OneNullRowRecordReader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Same as OneNullRowInputFormat, but with 0 rows. There's no way to store smth like OperatorDesc
 * in InputFormat, so this is how it is. We could perhaps encode the number of null rows in the
 * null path. However, NullIF can be used without using NullFS, so that would not be possible.
 */
public class ZeroRowsInputFormat extends NullRowsInputFormat
  implements VectorizedInputFormatInterface {

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter arg2) throws IOException {
    return new ZeroRowsRecordReader(conf, split);
  }

  public static class ZeroRowsRecordReader extends OneNullRowRecordReader {
    public ZeroRowsRecordReader(Configuration conf, InputSplit split) throws IOException {
      super(conf, split);
      processed = true;
    }
  }
}
