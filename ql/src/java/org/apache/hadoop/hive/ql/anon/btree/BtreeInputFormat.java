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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BtreeInputFormat implements InputFormat<NullWritable, KeyValueStruct> {

  private static final Logger LOG = LoggerFactory.getLogger(BtreeInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String dirs = job.get("mapred.input.dir");

    Path path = new Path(dirs);
    FileSystem fs = path.getFileSystem(job);
    List<FileStatus> statuses = HdfsUtils.listLocatedFileStatus(fs, path, null, false);

    List<BtreeSplit> splits = new ArrayList<>();
    for (FileStatus status : statuses) {
      BtreeSplit split = new BtreeSplit(status);
      splits.add(split);
    }

    return splits.toArray(new InputSplit[0]);
  }

  @Override
  public RecordReader<NullWritable, KeyValueStruct> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new BtreeRecordReader((BtreeSplit) split, job);
  }

  private static class BtreeRecordReader implements RecordReader<NullWritable, KeyValueStruct> {

    private final NullWritable key = NullWritable.get();
    private KeyValueStruct value = new KeyValueStruct();

    public BtreeRecordReader(BtreeSplit split, JobConf job) throws IOException {
    }

    @Override
    public boolean next(NullWritable key, KeyValueStruct value) throws IOException {
      return false;
    }

    @Override
    public NullWritable createKey() {
      return key;
    }

    @Override
    public KeyValueStruct createValue() {
      return value;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }

  }
}
