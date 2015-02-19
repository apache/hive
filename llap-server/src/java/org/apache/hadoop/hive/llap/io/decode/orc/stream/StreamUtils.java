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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils;

/**
 *
 */
public class StreamUtils {

  public static InStream createInStream(String streamName, String fileName, CompressionCodec codec,
      int bufferSize, EncodedColumnBatch.StreamBuffer streamBuffer, int compressionBufferIdx)
      throws IOException {
    if (streamBuffer == null) {
      return null;
    }

    int numBuffers = streamBuffer.cacheBuffers.size();
    List<DiskRange> input = new ArrayList<>(numBuffers);
    int totalLength = 0;

    // add the stream buffer (uncompressed) corresponding to the specified compression buffer index
    LlapMemoryBuffer lmb = streamBuffer.cacheBuffers.get(compressionBufferIdx);
    input.add(new RecordReaderImpl.CacheChunk(lmb, lmb.byteBuffer.position(),
        lmb.byteBuffer.limit()));
    totalLength += lmb.byteBuffer.remaining();

    // also add the next stream buffer as row group groups may span compression buffers
    if (compressionBufferIdx != streamBuffer.cacheBuffers.size() - 1) {
      lmb = streamBuffer.cacheBuffers.get(compressionBufferIdx + 1);
      input.add(new RecordReaderImpl.CacheChunk(lmb, lmb.byteBuffer.position(),
          lmb.byteBuffer.limit()));
      totalLength += lmb.byteBuffer.remaining();
    }

    return InStream.create(fileName, streamName, input, totalLength, codec, bufferSize, null);
  }

  public static InStream createInStream(String streamName, String fileName, CompressionCodec codec,
      int bufferSize, EncodedColumnBatch.StreamBuffer streamBuffer) throws IOException {
    if (streamBuffer == null) {
      return null;
    }

    int numBuffers = streamBuffer.cacheBuffers.size();
    List<DiskRange> input = new ArrayList<>(numBuffers);
    int totalLength = 0;
    for (int i = 0; i < numBuffers; i++) {
      LlapMemoryBuffer lmb = streamBuffer.cacheBuffers.get(i);
      input.add(new RecordReaderImpl.CacheChunk(lmb, 0, lmb.byteBuffer.limit()));
      totalLength += lmb.byteBuffer.limit();
    }
    return InStream.create(fileName, streamName, input, totalLength, codec, bufferSize, null);
  }

  public static PositionProvider getPositionProvider(OrcProto.RowIndexEntry rowIndex) {
    PositionProvider positionProvider = new RecordReaderImpl.PositionProviderImpl(rowIndex);
    return positionProvider;
  }

  /**
   * Returns compression buffer index within stream for the specified row group index. In other
   * words, it tells which compression buffer the specified row group belong. If row group spans
   * compression buffers then index of both compression buffers are return. If row group is in last
   * compression buffer then it never return two indices. For example:
   * |---------------CB0-------------|----------------CB1---------------|------------CB2-----------|
   * |--RG0--|--RG1--|--RG2--|--RG3--|--RG4--|--RG5--|--RG6--|--RG7--|--RG8--|--RG9--|--RG10--|RG11|
   *
   * Input: RG1  Output: [0, -1]
   * Input: RG3  Output: [0, -1]
   * Input: RG8  Output: [1,  2]
   * Input: RG11 Output: [2, -1]
   *
   * @param rgIdx - row group index
   * @param rowIndex - row index entries
   * @param columnEncoding - column encoding
   * @param colType - column type
   * @param streamKind - stream kind
   * @param hasNull - if present stream is present
   * @param isCompressed - if compressed
   * @return return int array with 2 elements. Both values will be non-negative only when row group
   * spans compression buffer.
   */
  public static int[] getCompressionBufferIndex(int rgIdx, OrcProto.RowIndex rowIndex,
      OrcProto.ColumnEncoding columnEncoding, OrcProto.Type colType,
      OrcProto.Stream.Kind streamKind, boolean hasNull, boolean isCompressed) {
    int[] result = new int[2];
    Arrays.fill(result, -1);

    // get total number of row group entries and see if the specified rgIdx is the last entry
    int numRgs = rowIndex.getEntryCount();
    int nextRgIdx = rgIdx == numRgs - 1 ? rgIdx : rgIdx + 1;
    OrcProto.RowIndexEntry rowIndexEntry = rowIndex.getEntry(rgIdx);
    OrcProto.RowIndexEntry nextRowIndexEntry = rowIndex.getEntry(nextRgIdx);

    // get the start index in position list for the specified stream
    int indexIx = RecordReaderUtils.getIndexPosition(columnEncoding.getKind(),
        colType.getKind(), streamKind, isCompressed, hasNull);

    // start offset of compression buffer corresponding to current row index
    long cbStartOffset = rowIndexEntry.getPositions(indexIx);

    // start offset of compression buffer corresponding to next row index
    long nextCbStartOffset = nextRowIndexEntry.getPositions(indexIx);

    // start offset of compression buffer corresponding to last row index
    long lastCbStartOffset = rowIndex.getEntry(numRgs - 1).getPositions(indexIx);

    boolean lastCompressionBuffer = false;
    int cbIdx = 0;
    long previosStartOffset = -1;
    boolean foundCompressionBufferIdx = false;

    for (OrcProto.RowIndexEntry entry : rowIndex.getEntryList()) {
      // initialze previous compression buffer start offset
      if (previosStartOffset == -1) {
        previosStartOffset = entry.getPositions(indexIx);
      }

      // get start offset of current row group index
      long currentStartOffset = entry.getPositions(indexIx);

      // if the start offset of compression buffer corresponding  to current row group index does
      // not match with the one specified then move on to next compression buffer
      if (currentStartOffset != previosStartOffset) {
        cbIdx++;
        previosStartOffset = currentStartOffset;
      }

      // if it matches, assign the compression buffer index within the stream to result
      if (currentStartOffset == cbStartOffset) {
        result[0] = cbIdx;
        foundCompressionBufferIdx = true;
      }

      // check if the current compression buffer is the last one in the stream
      if (currentStartOffset == lastCbStartOffset) {
        lastCompressionBuffer = true;
      }

      // if the current compression buffer offset does not match the next compression buffer offset
      // then it means the current row group is the last one in the compression buffer. In this
      // case, the current row group may span compression buffers, so include the next compression
      // buffer as well
      if ((cbStartOffset != nextCbStartOffset) && !lastCompressionBuffer) {
        result[1] = cbIdx + 1;
      }

      // we are done finding the compression buffer index
      if (foundCompressionBufferIdx) {
        break;
      }
    }
    return result;
  }
}
