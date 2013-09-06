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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RCFileMapReduceRecordReader<K extends LongWritable, V extends BytesRefArrayWritable>
  extends RecordReader<LongWritable, BytesRefArrayWritable> {

  private Reader in;
  private long start;
  private long end;
  private boolean more = true;

  // key and value objects are created once in initialize() and then reused
  // for every getCurrentKey() and getCurrentValue() call. This is important
  // since RCFile makes an assumption of this fact.

  private LongWritable key;
  private BytesRefArrayWritable value;

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public BytesRefArrayWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    more = next(key);
    if (more) {
      in.getCurrentRow(value);
    }

    return more;
  }

  private boolean next(LongWritable key) throws IOException {
    if (!more) {
      return false;
    }

    more = in.next(key);
    if (!more) {
      return false;
    }

    if (in.lastSeenSyncPos() >= end) {
      more = false;
      return more;
    }
    return more;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
    InterruptedException {

    FileSplit fSplit = (FileSplit) split;
    Path path = fSplit.getPath();
    Configuration conf = context.getConfiguration();
    this.in = new RCFile.Reader(path.getFileSystem(conf), path, conf);
    this.end = fSplit.getStart() + fSplit.getLength();

    if (fSplit.getStart() > in.getPosition()) {
      in.sync(fSplit.getStart());
    }

    this.start = in.getPosition();
    more = start < end;

    key = new LongWritable();
    value = new BytesRefArrayWritable();
  }
}
