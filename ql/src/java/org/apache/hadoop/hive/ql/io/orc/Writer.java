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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The interface for writing ORC files.
 */
public interface Writer {
  /**
   * Add arbitrary meta-data to the ORC file. This may be called at any point
   * until the Writer is closed. If the same key is passed a second time, the
   * second value will replace the first.
   * @param key a key to label the data with.
   * @param value the contents of the metadata.
   */
  void addUserMetadata(String key, ByteBuffer value);

  /**
   * Add a row to the ORC file.
   * @param row the row to add
   * @throws IOException
   */
  void addRow(Object row) throws IOException;

  /**
   * Flush all of the buffers and close the file. No methods on this writer
   * should be called afterwards.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Return the deserialized data size. Raw data size will be compute when
   * writing the file footer. Hence raw data size value will be available only
   * after closing the writer.
   *
   * @return raw data size
   */
  long getRawDataSize();

  /**
   * Return the number of rows in file. Row count gets updated when flushing
   * the stripes. To get accurate row count this method should be called after
   * closing the writer.
   *
   * @return row count
   */
  long getNumberOfRows();

  /**
   * Write an intermediate footer on the file such that if the file is
   * truncated to the returned offset, it would be a valid ORC file.
   * @return the offset that would be a valid end location for an ORC file
   */
  long writeIntermediateFooter() throws IOException;

  /**
   * Fast stripe append to ORC file. This interface is used for fast ORC file
   * merge with other ORC files. When merging, the file to be merged should pass
   * stripe in binary form along with stripe information and stripe statistics.
   * After appending last stripe of a file, use appendUserMetadata() to append
   * any user metadata.
   * @param stripe - stripe as byte array
   * @param offset - offset within byte array
   * @param length - length of stripe within byte array
   * @param stripeInfo - stripe information
   * @param stripeStatistics - stripe statistics (Protobuf objects can be
   *                         merged directly)
   * @throws IOException
   */
  public void appendStripe(byte[] stripe, int offset, int length,
      StripeInformation stripeInfo,
      OrcProto.StripeStatistics stripeStatistics) throws IOException;

  /**
   * When fast stripe append is used for merging ORC stripes, after appending
   * the last stripe from a file, this interface must be used to merge any
   * user metadata.
   * @param userMetadata - user metadata
   */
  public void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata);
}
