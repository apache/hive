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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

/**
 * <code>HiveOutputFormat</code> describes the output-specification for Hive's
 * operators. It has a method
 * {@link #getHiveRecordWriter(JobConf, Path, Class, boolean, Properties, Progressable)}
 * , with various parameters used to create the final out file and get some
 * specific settings.
 * 
 * @see org.apache.hadoop.mapred.OutputFormat
 * @see RecordWriter
 * @see JobConf
 */
public interface HiveOutputFormat<K extends WritableComparable, V extends Writable> {

  /**
   * create the final out file and get some specific settings.
   * 
   * @param jc
   *          the job configuration file
   * @param finalOutPath
   *          the final output file to be created
   * @param valueClass
   *          the value class used for create
   * @param isCompressed
   *          whether the content is compressed or not
   * @param tableProperties
   *          the table properties of this file's corresponding table
   * @param progress
   *          progress used for status report
   * @return the RecordWriter for the output file
   */
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException;

}
