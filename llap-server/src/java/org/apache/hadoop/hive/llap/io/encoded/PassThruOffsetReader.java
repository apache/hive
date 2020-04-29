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
package org.apache.hadoop.hive.llap.io.encoded;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.encoded.SerDeEncodedDataReader.ReaderWithOffsets;
import org.apache.hadoop.hive.ql.exec.FooterBuffer;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("rawtypes") class PassThruOffsetReader implements ReaderWithOffsets {
  protected final RecordReader sourceReader;
  protected final Object key;
  protected final Writable value;
  protected final JobConf jobConf;
  protected final int skipHeaderCnt;
  protected final int skipFooterCnt;
  private transient FooterBuffer footerBuffer;
  private transient boolean initialized = false;

  PassThruOffsetReader(RecordReader sourceReader, JobConf jobConf, int headerCnt, int footerCnt) {
    this.sourceReader = sourceReader;
    this.key = sourceReader.createKey();
    this.value = (Writable)sourceReader.createValue();
    this.jobConf = jobConf;
    this.skipHeaderCnt = headerCnt;
    this.skipFooterCnt = footerCnt;
  }

  @Override
  public boolean next() throws IOException {
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
        opNotEOF = Utilities.skipHeader(sourceReader, skipFooterCnt, key, value);

        // Initialize footer buffer.
        if (opNotEOF && skipFooterCnt > 0) {
          footerBuffer = new FooterBuffer();
          opNotEOF = footerBuffer.initializeBuffer(jobConf, sourceReader, skipFooterCnt, (WritableComparable) key, value);
        }
        this.initialized = true;
      }

      if (opNotEOF && footerBuffer == null) {
        /**
         * When file doesn't end after skipping header line
         * and there is NO footer lines, read normally.
         */
        opNotEOF = sourceReader.next(key, value);
      }

      if (opNotEOF && footerBuffer != null) {
        /**
         * When file doesn't end after skipping header line
         * and there IS footer lines, update footerBuffer
         */
        opNotEOF = footerBuffer.updateBuffer(jobConf, sourceReader, (WritableComparable) key, value);
      }

      if (opNotEOF) {
        // File reached the end
        return true;
      } else {
        // Done reading
        return false;
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Writable getCurrentRow() {
    return value;
  }

  @Override
  public void close() throws IOException {
    sourceReader.close();
  }

  @Override
  public long getCurrentRowStartOffset() {
    return -1;
  }

  @Override
  public long getCurrentRowEndOffset() {
    return -1;
  }

  @Override
  public boolean hasOffsets() {
    return false;
  }
}