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

import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * BucketizedHiveRecordReader is a wrapper on a list of RecordReader. It behaves
 * similar as HiveRecordReader while it wraps a list of RecordReader from one
 * file.
 */
public class BucketizedHiveRecordReader<K extends WritableComparable, V extends Writable>
    extends HiveContextAwareRecordReader<K, V> {
  protected final BucketizedHiveInputSplit split;
  protected final InputFormat inputFormat;
  protected final Reporter reporter;
  protected long progress;
  protected int idx;

  public BucketizedHiveRecordReader(InputFormat inputFormat,
      BucketizedHiveInputSplit bucketizedSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    super(jobConf);

    this.split = bucketizedSplit;
    this.inputFormat = inputFormat;
    this.reporter = reporter;
    initNextRecordReader();
  }

  @Override
  public void doClose() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
    idx = 0;
  }

  public K createKey() {
    return (K) recordReader.createKey();
  }

  public V createValue() {
    return (V) recordReader.createValue();
  }

  public long getPos() throws IOException {
    if (recordReader != null) {
      return recordReader.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public float getProgress() throws IOException {
    // The calculation is strongly dependent on the assumption that all splits
    // came from the same file
    return Math.min(1.0f, ((recordReader == null || this.getIOContext().isBinarySearching()) ?
        progress : recordReader.getPos()) / (float) (split.getLength()));
  }

  @Override
  public boolean doNext(K key, V value) throws IOException {
    while ((recordReader == null) || !doNextWithExceptionHandler(key, value)) {
      if (!initNextRecordReader()) {
        return false;
      }
    }
    return true;
  }

  private boolean doNextWithExceptionHandler(K key, V value) throws IOException {
    return super.doNext(key, value);
  }

  /**
   * Get the record reader for the next chunk in this
   * BucketizedHiveRecordReader.
   */
  protected boolean initNextRecordReader() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
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
      recordReader = inputFormat.getRecordReader(split.getSplit(idx), jobConf,
          reporter);
    } catch (Exception e) {
      recordReader = HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(e, jobConf);
    }

    // if we're performing a binary search, we need to restart it
    if (isSorted) {
      initIOContextSortedProps((FileSplit) split.getSplit(idx), recordReader, jobConf);
    }
    idx++;
    return true;
  }


}
