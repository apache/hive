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

package org.apache.hadoop.hive.ql.anon.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnonInputFormat extends FileInputFormat<NullWritable, Text> {

  private Configuration conf;

  public AnonInputFormat() {
    super();
  }

  @Override
  public RecordReader<NullWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = ShimLoader.getHadoopShims().getConfiguration(context);
    AnonInputSplit dummyInputSplit = (AnonInputSplit) inputSplit;
    return new AnonRecordReader(conf);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    conf = job.getConfiguration();

    final List<FileStatus> statuses = listStatus(job);
    final List<InputSplit> splits = new ArrayList<>();

    for (FileStatus status : statuses) {

      long length = status.getLen();
      Path path = status.getPath();
      BlockLocation[] blkLocations;
      if (status instanceof LocatedFileStatus) {
        blkLocations = ((LocatedFileStatus) status).getBlockLocations();

      } else {
        FileSystem fs = path.getFileSystem(conf);
        blkLocations = fs.getFileBlockLocations(status, 0, length);
      }

      AnonInputSplit split = new AnonInputSplit(path);
      split.setLength(length);
      splits.add(split);
    }

    return splits;
  }
}
