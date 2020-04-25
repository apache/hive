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
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes") class PassThruOffsetReader implements ReaderWithOffsets {
  public static final Logger LOG = LoggerFactory.getLogger("LlapIoImpl");
  protected RecordReader sourceReader;
  protected final Object key;
  protected final Writable value;
  protected final TableDesc tableDesc;
  protected final JobConf jobConf;
  private transient int headerCount;
  private transient  int footerCount;
  private transient FooterBuffer footerBuffer;
  private transient boolean initialized = false;

  PassThruOffsetReader(RecordReader sourceReader, TableDesc currTD, JobConf jobConf) {
    this.sourceReader = sourceReader;
    key = sourceReader.createKey();
    value = (Writable)sourceReader.createValue();
    tableDesc = currTD;
    this.jobConf = jobConf;
    LOG.info("PANOS HERE Reader with Table {} ", tableDesc);
  }

  @Override
  public boolean next() throws IOException {
    try {
      boolean opNotEOF = true;
      if (!initialized) {
        /**
         * Start reading a new file.
         * If file contains header, skip header lines before reading the records.
         * If file contains footer, used FooterBuffer to cache and remove footer
         * records at the end of the file.
         */
        headerCount = Utilities.getHeaderCount(tableDesc);
        footerCount = Utilities.getFooterCount(tableDesc, jobConf);

        // Skip header lines.
        opNotEOF = Utilities.skipHeader(sourceReader, headerCount, key, value);

        // Initialize footer buffer.
        if (opNotEOF && footerCount > 0) {
          footerBuffer = new FooterBuffer();
          opNotEOF = footerBuffer.initializeBuffer(jobConf, sourceReader, footerCount, (WritableComparable) key, value);
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
        close();
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