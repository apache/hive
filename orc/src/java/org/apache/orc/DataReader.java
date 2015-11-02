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

package org.apache.orc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.common.io.DiskRangeList;

/** An abstract data reader that IO formats can use to read bytes from underlying storage. */
public interface DataReader {

  /** Opens the DataReader, making it ready to use. */
  void open() throws IOException;

  /** Closes the DataReader. */
  void close() throws IOException;

  /** Reads the data.
   *
   * Note that for the cases such as zero-copy read, caller must release the disk ranges
   * produced after being done with them. Call isTrackingDiskRanges to find out if this is needed.
   * @param range List if disk ranges to read. Ranges with data will be ignored.
   * @param baseOffset Base offset from the start of the file of the ranges in disk range list.
   * @param doForceDirect Whether the data should be read into direct buffers.
   * @return New or modified list of DiskRange-s, where all the ranges are filled with data.
   */
  DiskRangeList readFileData(
      DiskRangeList range, long baseOffset, boolean doForceDirect) throws IOException;


  /**
   * Whether the user should release buffers created by readFileData. See readFileData javadoc.
   */
  boolean isTrackingDiskRanges();

  /**
   * Releases buffers created by readFileData. See readFileData javadoc.
   * @param toRelease The buffer to release.
   */
  void releaseBuffer(ByteBuffer toRelease);
}
