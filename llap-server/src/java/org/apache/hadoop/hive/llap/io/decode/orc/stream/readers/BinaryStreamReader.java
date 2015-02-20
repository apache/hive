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
package org.apache.hadoop.hive.llap.io.decode.orc.stream.readers;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.StreamUtils;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

/**
 *
 */
public class BinaryStreamReader extends RecordReaderImpl.BinaryTreeReader {
  private boolean isFileCompressed;

  private BinaryStreamReader(int columnId, InStream present,
      InStream data, InStream length, boolean isFileCompressed,
      OrcProto.ColumnEncoding encoding, OrcProto.RowIndexEntry rowIndex) throws IOException {
    super(columnId, present, data, length, encoding);
    this.isFileCompressed = isFileCompressed;

    // position the readers based on the specified row index
    seek(StreamUtils.getPositionProvider(rowIndex));
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    if (present != null) {
      if (isFileCompressed) {
        index.getNext();
      }
      present.seek(index);
    }

    if (isFileCompressed) {
      index.getNext();
    }
    stream.seek(index);

    if (isFileCompressed) {
      index.getNext();
    }
    lengths.seek(index);
  }

  public static class StreamReaderBuilder {
    private String fileName;
    private int columnIndex;
    private EncodedColumnBatch.StreamBuffer presentStream;
    private EncodedColumnBatch.StreamBuffer dataStream;
    private EncodedColumnBatch.StreamBuffer lengthStream;
    private CompressionCodec compressionCodec;
    private int bufferSize;
    private OrcProto.RowIndexEntry rowIndex;
    private OrcProto.ColumnEncoding columnEncoding;

    public StreamReaderBuilder setFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public StreamReaderBuilder setColumnIndex(int columnIndex) {
      this.columnIndex = columnIndex;
      return this;
    }

    public StreamReaderBuilder setPresentStream(EncodedColumnBatch.StreamBuffer presentStream) {
      this.presentStream = presentStream;
      return this;
    }

    public StreamReaderBuilder setDataStream(EncodedColumnBatch.StreamBuffer dataStream) {
      this.dataStream = dataStream;
      return this;
    }

    public StreamReaderBuilder setLengthStream(EncodedColumnBatch.StreamBuffer secondaryStream) {
      this.lengthStream = secondaryStream;
      return this;
    }

    public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
      this.compressionCodec = compressionCodec;
      return this;
    }

    public StreamReaderBuilder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public StreamReaderBuilder setRowIndex(OrcProto.RowIndexEntry rowIndex) {
      this.rowIndex = rowIndex;
      return this;
    }

    public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
      this.columnEncoding = encoding;
      return this;
    }

    public BinaryStreamReader build() throws IOException {
      InStream present = StreamUtils.createInStream(OrcProto.Stream.Kind.PRESENT.name(), fileName,
            null, bufferSize, presentStream);

      InStream data = StreamUtils.createInStream(OrcProto.Stream.Kind.DATA.name(), fileName,
            null, bufferSize, dataStream);

      InStream length = StreamUtils.createInStream(OrcProto.Stream.Kind.LENGTH.name(), fileName,
            null, bufferSize, lengthStream);

      boolean isFileCompressed = compressionCodec != null;
      return new BinaryStreamReader(columnIndex, present, data, length, isFileCompressed,
          columnEncoding, rowIndex);
    }
  }

  public static StreamReaderBuilder builder() {
    return new StreamReaderBuilder();
  }
}
