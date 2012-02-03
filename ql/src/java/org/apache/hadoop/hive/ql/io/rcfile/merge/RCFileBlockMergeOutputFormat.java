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

package org.apache.hadoop.hive.ql.io.rcfile.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class RCFileBlockMergeOutputFormat extends
    FileOutputFormat<RCFileKeyBufferWrapper, RCFileValueBufferWrapper> {

  public static void setMergeOutputPath(JobConf job, Path path) {
    job.set("hive.rcfile.merge.output.dir", path.toString());
  }
  
  public static Path getMergeOutputPath(JobConf conf) {
    String name = conf.get("hive.rcfile.merge.output.dir");
    return name == null ? null: new Path(name);
  }

  @Override
  public RecordWriter<RCFileKeyBufferWrapper, RCFileValueBufferWrapper> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    throw new RuntimeException("Not implemented.");
  }
}