/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class JdbcOutputFormat implements OutputFormat<NullWritable, MapWritable>,
                                         HiveOutputFormat<NullWritable, MapWritable> {

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Write operations are not allowed.");
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem ignored,
      JobConf job,
      String name,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Write operations are not allowed.");
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // do nothing
  }

}
