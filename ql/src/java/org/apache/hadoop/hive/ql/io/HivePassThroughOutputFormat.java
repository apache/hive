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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 *  This pass through class is used to wrap OutputFormat implementations such that new OutputFormats not derived from
 *  HiveOutputFormat gets through the checker
 */
public class HivePassThroughOutputFormat<K, V> implements HiveOutputFormat<K, V>{

  private final OutputFormat<?, ?> actualOutputFormat;

  public HivePassThroughOutputFormat(OutputFormat<?, ?> outputFormat) {
    actualOutputFormat = outputFormat;
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    actualOutputFormat.checkOutputSpecs(ignored, job);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(FileSystem ignored,
       JobConf job, String name, Progressable progress) throws IOException {
    return (RecordWriter<K, V>) actualOutputFormat.getRecordWriter(ignored,
                 job, name, progress);
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    if (actualOutputFormat instanceof HiveOutputFormat) {
      return ((HiveOutputFormat<K, V>) actualOutputFormat).getHiveRecordWriter(jc,
           finalOutPath, valueClass, isCompressed, tableProperties, progress);
    }
    FileSystem fs = finalOutPath.getFileSystem(jc);
    RecordWriter<?, ?> recordWriter = actualOutputFormat.getRecordWriter(fs, jc, null, progress);
    return new HivePassThroughRecordWriter(recordWriter);
  }
}
