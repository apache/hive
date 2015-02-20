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

import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 *
 */
public class StreamUtils {

  public static InStream createInStream(String streamName, String fileName, CompressionCodec codec,
      int bufferSize, EncodedColumnBatch.StreamBuffer streamBuffer) throws IOException {
    if (streamBuffer == null) {
      return null;
    }

    int numBuffers = streamBuffer.cacheBuffers.size();
    List<ByteBuffer> input = Lists.newArrayList();
    List<Long> offsetsList = Lists.newArrayList();
    long totalLength = 0;
    for (int i = 0; i < numBuffers; i++) {
      ByteBuffer data = streamBuffer.cacheBuffers.get(i).byteBuffer.duplicate();
      input.add(data);
      offsetsList.add(totalLength);
      totalLength += data.remaining();
    }
    ByteBuffer[] buffers = input.toArray(new ByteBuffer[input.size()]);
    long[] offsets = Longs.toArray(offsetsList);
    return InStream.create(fileName, streamName, buffers, offsets, totalLength, codec, bufferSize);
  }

  public static PositionProvider getPositionProvider(OrcProto.RowIndexEntry rowIndex) {
    PositionProvider positionProvider = new RecordReaderImpl.PositionProviderImpl(rowIndex);
    return positionProvider;
  }
}
