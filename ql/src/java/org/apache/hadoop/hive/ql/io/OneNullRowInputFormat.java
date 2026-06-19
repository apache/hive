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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * OneNullRowInputFormat outputs one null row. Used in implementation of
 * metadata only queries.
 *
 */
public class OneNullRowInputFormat extends NullRowsInputFormat
  implements VectorizedInputFormatInterface {

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter arg2) throws IOException {
    return new OneNullRowRecordReader(conf, split);
  }

  public static class OneNullRowRecordReader extends NullRowsRecordReader {
    public OneNullRowRecordReader(Configuration conf, InputSplit split) throws IOException {
      super(conf, split);
    }

    protected boolean processed;

    @Override
    public long getPos() throws IOException {
      return processed ? 1 : 0;
    }

    @Override
    public float getProgress() throws IOException {
      return processed ? 1.0f : 0f;
    }

    @Override
    public boolean next(Object key, Object value) throws IOException {
      if (processed) {
        return false;
      }
      processed = true;
      if (rbCtx != null) {
        makeNullVrb(value, 1);
      }
      return true;
    }
  }
}
