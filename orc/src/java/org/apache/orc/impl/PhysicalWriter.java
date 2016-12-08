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

package org.apache.orc.impl;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.orc.OrcProto.BloomFilterIndex;
import org.apache.orc.OrcProto.Footer;
import org.apache.orc.OrcProto.Metadata;
import org.apache.orc.OrcProto.PostScript;
import org.apache.orc.OrcProto.RowIndex;
import org.apache.orc.OrcProto.StripeFooter;
import org.apache.orc.OrcProto.StripeInformation;

public interface PhysicalWriter {

  /**
   * Creates all the streams/connections/etc. necessary to write.
   */
  void initialize() throws IOException;

  /**
   * Writes out the file metadata.
   * @param builder Metadata builder to finalize and write.
   */
  void writeFileMetadata(Metadata.Builder builder) throws IOException;

  /**
   * Writes out the file footer.
   * @param builder Footer builder to finalize and write.
   */
  void writeFileFooter(Footer.Builder builder) throws IOException;

  /**
   * Writes out the postscript (including the size byte if needed).
   * @param builder Postscript builder to finalize and write.
   */
  void writePostScript(PostScript.Builder builder) throws IOException;

  /**
   * Creates physical stream to write data to.
   * @param name Stream name.
   * @return The output stream.
   */
  OutStream getOrCreatePhysicalStream(StreamName name) throws IOException;

  /**
   * Flushes the data in all the streams, spills them to disk, write out stripe footer.
   * @param footer Stripe footer to be updated with relevant data and written out.
   * @param dirEntry File metadata entry for the stripe, to be updated with relevant data.
   */
  void finalizeStripe(StripeFooter.Builder footer,
      StripeInformation.Builder dirEntry) throws IOException;

  /**
   * Writes out the index for the stripe column.
   * @param streamName Stream name.
   * @param rowIndex Row index entries to write.
   */
  void writeIndexStream(StreamName name, RowIndex.Builder rowIndex) throws IOException;

  /**
   * Writes out the index for the stripe column.
   * @param streamName Stream name.
   * @param bloomFilterIndex Bloom filter index to write.
   */
  void writeBloomFilterStream(StreamName streamName,
      BloomFilterIndex.Builder bloomFilterIndex) throws IOException;

  /**
   * Closes the writer.
   */
  void close() throws IOException;

  /**
   * Force-flushes the writer.
   */
  void flush() throws IOException;

  /**
   * @return the physical writer position (e.g. for updater).
   */
  long getRawWriterPosition() throws IOException;

  /** @return physical stripe size, taking padding into account. */
  long getPhysicalStripeSize();

  /** @return whether the writer is compressed. */
  boolean isCompressed();

  /**
   * Appends raw stripe data (e.g. for file merger).
   * @param stripe Stripe data buffer.
   * @param offset Stripe data buffer offset.
   * @param length Stripe data buffer length.
   * @param dirEntry File metadata entry for the stripe, to be updated with relevant data.
   * @throws IOException
   */
  void appendRawStripe(byte[] stripe, int offset, int length,
      StripeInformation.Builder dirEntry) throws IOException;

  /**
   * @return the estimated memory usage for the stripe.
   */
  long estimateMemory();
}
