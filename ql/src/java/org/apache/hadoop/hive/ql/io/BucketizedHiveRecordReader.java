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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/**
 * BucketizedHiveRecordReader is a wrapper on a list of RecordReader. It behaves
 * similar as HiveRecordReader while it wraps a list of RecordReader from one
 * file.
 */
public class BucketizedHiveRecordReader<K extends WritableComparable, V extends Writable>
    extends HiveContextAwareRecordReader<K, V> {
  protected final BucketizedHiveInputSplit split;
  protected final InputFormat inputFormat;
  protected final JobConf jobConf;
  protected final Reporter reporter;
  protected RecordReader curReader;
  protected long progress;
  protected int idx;

  public BucketizedHiveRecordReader(InputFormat inputFormat,
      BucketizedHiveInputSplit bucketizedSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    this.split = bucketizedSplit;
    this.inputFormat = inputFormat;
    this.jobConf = jobConf;
    this.reporter = reporter;
    initNextRecordReader();
  }

  public void doClose() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
    idx = 0;
  }

  public K createKey() {
    return (K) curReader.createKey();
  }

  public V createValue() {
    return (V) curReader.createValue();
  }

  public long getPos() throws IOException {
    if (curReader != null) {
      return curReader.getPos();
    } else {
      return 0;
    }
  }

  public float getProgress() throws IOException {
    // The calculation is strongly dependent on the assumption that all splits
    // came from the same file
    return Math.min(1.0f, ((curReader == null) ? progress : curReader.getPos())
        / (float) (split.getLength()));
  }

  public boolean doNext(K key, V value) throws IOException {
    while ((curReader == null) || !curReader.next(key, value)) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the record reader for the next chunk in this
   * BucketizedHiveRecordReader.
   */
  protected boolean initNextRecordReader() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx - 1); // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumSplits()) {
      return false;
    }

    // get a record reader for the idx-th chunk
    try {
      curReader = inputFormat.getRecordReader(split.getSplit(idx), jobConf,
          reporter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    idx++;
    return true;
  }
}
