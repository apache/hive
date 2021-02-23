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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.StripeInformation;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

public interface EncodedReader {

  /**
   * Reads encoded data from ORC file.
   * @param stripeIx Index of the stripe to read.
   * @param stripe Externally provided metadata (from metadata reader or external cache).
   * @param index Externally provided metadata (from metadata reader or external cache).
   * @param encodings Externally provided metadata (from metadata reader or external cache).
   * @param streams Externally provided metadata (from metadata reader or external cache).
   * @param physicalFileIncludes The array of booleans indicating whether each column should be read.
   * @param rgs Arrays of rgs, per column set to true in included, that are to be read.
   *               null in each respective position means all rgs for this column need to be read.
   * @param consumer The sink for data that has been read.
   */
  void readEncodedColumns(int stripeIx, StripeInformation stripe,
      OrcProto.RowIndex[] index, List<OrcProto.ColumnEncoding> encodings,
      List<OrcProto.Stream> streams, boolean[] physicalFileIncludes, boolean[] rgs,
      Consumer<OrcEncodedColumnBatch> consumer) throws IOException;

  /**
   * Closes the reader.
   */
  void close() throws IOException;

  /**
   * Controls the low-level debug tracing. (Hopefully) allows for optimization where tracing
   * checks are entirely eliminated because this method is called with constant value, similar
   * to just checking the constant in the first place.
   */
  void setTracing(boolean isEnabled);

  /**
   * Read the indexes from ORC file.
   * @param index The destination with pre-allocated arrays to put index data into.
   * @param stripe Externally provided metadata (from metadata reader or external cache).
   * @param streams Externally provided metadata (from metadata reader or external cache).
   * @param included The array of booleans indicating whether each column should be read. 
   * @param sargColumns The array of booleans indicating whether each column's
   *                    bloom filters should be read.
   */
  void readIndexStreams(OrcIndex index, StripeInformation stripe,
      List<OrcProto.Stream> streams, boolean[] included, boolean[] sargColumns)
          throws IOException;

  void setStopped(AtomicBoolean isStopped);

  /**
   * Reads the encoded data from ORC file by disk ranges and populates the cache.
   */
  void preReadDataRanges(DiskRangeList ranges) throws IOException;

}
