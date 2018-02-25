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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * RCFileInputFormat.
 *
 * @param <K>
 * @param <V>
 */
public class RCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable>
    extends FileInputFormat<K, V> implements InputFormatChecker {

  public RCFileInputFormat() {
    setMinSplitSize(SequenceFile.SYNC_INTERVAL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    reporter.setStatus(split.toString());

    return new RCFileRecordReader(job, (FileSplit) split);
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
      List<FileStatus> files) throws IOException {
    if (files.size() <= 0) {
      return false;
    }
    for (int fileId = 0; fileId < files.size(); fileId++) {
      RCFile.Reader reader = null;
      try {
        reader = new RCFile.Reader(fs, files.get(fileId)
            .getPath(), conf);
        reader.close();
        reader = null;
      } catch (IOException e) {
        return false;
      } finally {
        if (null != reader) {
          reader.close();
        }
      }
    }
    return true;
  }
}
