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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

class RecordReaderWrapper extends LineRecordReader {
  private static final Method isCompressedMethod;
  private transient boolean initialized = false;
  protected final JobConf jobConf;
  protected final int skipHeaderCnt;
  protected final int skipFooterCnt;

  private List<Pair<WritableComparable, Writable>> footerBuffer;
  private int cur;

  private static final Logger LOG = LoggerFactory.getLogger(RecordReaderWrapper.class.getName());

  static {
    Method isCompressedMethodTmp;
    try {
      isCompressedMethodTmp = LineRecordReader.class.getDeclaredMethod("isCompressedInput");
      isCompressedMethodTmp.setAccessible(true);
    } catch (Throwable t) {
      isCompressedMethodTmp = null;
      LOG.warn("Cannot get LineRecordReader isCompressedInput method", t);
    }
    isCompressedMethod = isCompressedMethodTmp;
  }

  static RecordReader create(InputFormat inputFormat, HiveInputFormat.HiveInputSplit split, TableDesc tableDesc,
      JobConf jobConf, Reporter reporter) throws IOException {
    int headerCount = Utilities.getHeaderCount(tableDesc);
    int footerCount = Utilities.getFooterCount(tableDesc, jobConf);
    RecordReader innerReader = inputFormat.getRecordReader(split.getInputSplit(), jobConf, reporter);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Using {} to read data with skip.header.line.count {} and skip.footer.line.count {}",
          innerReader.getClass().getSimpleName(), headerCount, footerCount);
    }

    // For non-compressed Text Files Header/Footer Skipping is already done as part of SkippingTextInputFormat
    if (innerReader instanceof LineRecordReader) {
      // File not compressed, skipping is already done as part of SkippingTextInputFormat
      if (isCompressedMethod == null) {
        return innerReader;
      }
      Boolean isCompressed = null;
      try {
        isCompressed = (Boolean) isCompressedMethod.invoke(innerReader);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        LOG.error("Cannot check the reader for compression; offsets not supported", e);
        return innerReader;
      }
      if (isCompressed && (headerCount > 0 || footerCount > 0)) {
        // Cannot slice compressed files - do header/footer skipping within the Reader
        LOG.info("Reader is compressed; offsets not supported");
        return new RecordReaderWrapper(split, jobConf, headerCount, footerCount);
      }
      if (headerCount > 0 && split.getStart() == 0) {
        // Skipping empty/null lines leading to Split start -1 being zero
        LOG.info("Reader with blank head line(s)");
        return new RecordReaderWrapper(split, jobConf, headerCount, footerCount);
      }
    }
    return innerReader;
  }

  private RecordReaderWrapper(FileSplit split, JobConf jobConf, int headerCnt, int footerCnt) throws IOException {
    super(jobConf, split);
    this.jobConf = jobConf;
    this.skipHeaderCnt = headerCnt;
    this.skipFooterCnt = footerCnt;
  }

  @Override
  public synchronized boolean next(LongWritable key, Text value) throws IOException {
    try {
      boolean opNotEOF = true;
      /**
       * Start reading a new file.
       * If file contains header, skip header lines before reading the records.
       * If file contains footer, used FooterBuffer to cache and remove footer
       * records at the end of the file.
       */
      if (!initialized) {
        // Skip header lines.
        opNotEOF = skipHeader(skipHeaderCnt, key, value);

        // Initialize footer buffer.
        if (opNotEOF && skipFooterCnt > 0) {
          opNotEOF = initializeBuffer(jobConf, skipFooterCnt, key, value);
        }
        this.initialized = true;
      }

      if (opNotEOF && footerBuffer == null) {
        /**
         * When file doesn't end after skipping header line
         * and there is NO footer lines, read normally.
         */
        opNotEOF = super.next(key, value);
      }

      if (opNotEOF && footerBuffer != null) {
        /**
         * When file doesn't end after skipping header line
         * and there IS footer lines, update footerBuffer
         */
        opNotEOF = updateBuffer(jobConf, key, value);
      }

      if (opNotEOF) {
        // File reached the end
        return true;
      }
      // Done reading
      return false;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private boolean skipHeader(int headerCount, LongWritable key, Text value) throws IOException {
    while (headerCount > 0) {
      if (!super.next(key, value)) {
        return false;
      }
      headerCount--;
    }
    return true;
  }

  public boolean initializeBuffer(JobConf job, int footerCount, LongWritable key, Text value) throws IOException {
    // Fill the buffer with key value pairs.
    this.footerBuffer = new ArrayList<>();
    while (footerBuffer.size() < footerCount) {
      boolean notEOF = super.next(key, value);
      if (!notEOF) {
        return false;
      }
      WritableComparable left = ReflectionUtils.copy(job, key, null);
      Writable right = ReflectionUtils.copy(job, value, null);
      Pair<WritableComparable, Writable> tem = Pair.of(left, right);
      footerBuffer.add(tem);
    }
    this.cur = 0;
    return true;
  }

  /**
   * Enqueue most recent record read, and dequeue earliest result in the queue.
   *
   * @param job
   *          Current job configuration.
   * @param key
   *          Key of current reading record.
   *
   * @param value
   *          Value of current reading record.
   *
   * @return Return false if reaches the end of file, otherwise return true.
   */
  public boolean updateBuffer(JobConf job, WritableComparable key, Writable value) throws IOException {
    key = ReflectionUtils.copy(job, footerBuffer.get(cur).getKey(), key);
    value = ReflectionUtils.copy(job, footerBuffer.get(cur).getValue(), value);
    boolean notEOF = super.next((LongWritable) footerBuffer.get(cur).getKey(), (Text) footerBuffer.get(cur).getValue());
    if (notEOF) {
      cur = (++cur) % footerBuffer.size();
    }
    return notEOF;
  }
}
