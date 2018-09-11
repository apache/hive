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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * https://cwiki.apache.org/confluence/display/Hive/TeradataBinarySerde.
 * FileInputFormat for Teradata binary files.
 *
 * In the Teradata Binary File, each record constructs as below:
 * The first 2 bytes represents the length of the bytes next for this record.
 * Then the null bitmap whose length is depended on the number of fields is followed.
 * Then each field of the record is serialized into bytes - the serialization strategy is decided by the type of field.
 * At last, there is one byte (0x0a) in the end of the record.
 *
 * This InputFormat currently doesn't support the split of the file.
 * Teradata binary files are using little endian.
 */
public class TeradataBinaryFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

  @Override public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new TeradataBinaryRecordReader(job, (FileSplit) split);
  }

  /**
   * the <code>TeradataBinaryFileInputFormat</code> is not splittable right now.
   * Override the <code>isSplitable</code> function.
   *
   * @param fs the file system that the file is on
   * @param filename the file name to check
   * @return is this file splitable?
   */
  @Override protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}
