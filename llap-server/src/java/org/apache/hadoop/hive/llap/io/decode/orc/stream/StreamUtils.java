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
package org.apache.hadoop.hive.llap.io.decode.orc.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

import com.google.common.collect.Lists;

/**
 * Stream utility.
 */
public class StreamUtils {

  /**
   * Create LlapInStream from stream buffer.
   *
   * @param streamName - stream name
   * @param fileName - file name
   * @param streamBuffer - stream buffer
   * @return - LlapInStream
   * @throws IOException
   */
  public static SettableUncompressedStream createLlapInStream(String streamName, String fileName,
      EncodedColumnBatch.StreamBuffer streamBuffer) throws IOException {
    if (streamBuffer == null) {
      return null;
    }

    List<DiskRange> diskRanges = Lists.newArrayList();
    long totalLength = createDiskRanges(streamBuffer, diskRanges);
    return new SettableUncompressedStream(fileName, streamName, diskRanges, totalLength);
  }

  /**
   * Converts row index entry to position provider.
   *
   * @param rowIndex - row index entry
   * @return - position provider
   */
  public static PositionProvider getPositionProvider(OrcProto.RowIndexEntry rowIndex) {
    PositionProvider positionProvider = new RecordReaderImpl.PositionProviderImpl(rowIndex);
    return positionProvider;
  }

  /**
   * Converts stream buffers to disk ranges.
   * @param streamBuffer - stream buffer
   * @param diskRanges - initial empty list of disk ranges
   * @return - total length of disk ranges
   */
  public static long createDiskRanges(EncodedColumnBatch.StreamBuffer streamBuffer,
      List<DiskRange> diskRanges) {
    long totalLength = 0;
    for (LlapMemoryBuffer memoryBuffer : streamBuffer.cacheBuffers) {
      ByteBuffer buffer = memoryBuffer.byteBuffer.duplicate();
      RecordReaderImpl.BufferChunk bufferChunk = new RecordReaderImpl.BufferChunk(buffer,
          totalLength);
      diskRanges.add(bufferChunk);
      totalLength += buffer.remaining();
    }
    return totalLength;
  }
}
