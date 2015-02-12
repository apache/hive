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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.BufferChunk;

import com.google.common.collect.Lists;

public class MetadataReader {
  private final FSDataInputStream file;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final int typeCount;

  public MetadataReader(FileSystem fileSystem, Path path,
      CompressionCodec codec, int bufferSize, int typeCount) throws IOException {
    this(fileSystem.open(path), codec, bufferSize, typeCount);
  }

  public MetadataReader(FSDataInputStream file,
      CompressionCodec codec, int bufferSize, int typeCount) {
    this.file = file;
    this.codec = codec;
    this.bufferSize = bufferSize;
    this.typeCount = typeCount;
  }

  public OrcProto.RowIndex[] readRowIndex(StripeInformation stripe, OrcProto.StripeFooter footer,
      boolean[] included, OrcProto.RowIndex[] indexes) throws IOException {
    if (footer == null) {
      footer = readStripeFooter(stripe);
    }
    if (indexes == null) {
      indexes = new OrcProto.RowIndex[typeCount];
    }
    long offset = stripe.getOffset();
    for (OrcProto.Stream stream : footer.getStreamsList()) {
      if (stream.getKind() == OrcProto.Stream.Kind.ROW_INDEX) {
        int col = stream.getColumn();
        if ((included != null && !included[col]) || indexes[col] != null) continue;
        byte[] buffer = new byte[(int) stream.getLength()];
        file.seek(offset);
        file.readFully(buffer);
        indexes[col] = OrcProto.RowIndex.parseFrom(InStream.create(null, "index",
            Lists.<DiskRange>newArrayList(new BufferChunk(ByteBuffer.wrap(buffer), 0)),
            stream.getLength(), codec, bufferSize, null));
      }
      offset += stream.getLength();
    }
    return indexes;
  }

  public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
    int tailLength = (int) stripe.getFooterLength();

    // read the footer
    ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
    file.seek(offset);
    file.readFully(tailBuf.array(), tailBuf.arrayOffset(), tailLength);
    return OrcProto.StripeFooter.parseFrom(InStream.create(null, "footer",
        Lists.<DiskRange>newArrayList(new BufferChunk(tailBuf, 0)),
        tailLength, codec, bufferSize, null));
  }

  public void close() throws IOException {
    file.close();
  }
}
