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
 * Stream reader for timestamp column type.
 */
public class TimestampStreamReader extends RecordReaderImpl.TimestampTreeReader {
  private boolean isFileCompressed;

  private TimestampStreamReader(int columnId, InStream present,
      InStream data, InStream nanos, boolean isFileCompressed,
      OrcProto.ColumnEncoding encoding, boolean skipCorrupt,
      OrcProto.RowIndexEntry rowIndex) throws IOException {
    super(columnId, present, data, nanos, encoding, skipCorrupt);
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
    data.seek(index);

    if (isFileCompressed) {
      index.getNext();
    }
    nanos.seek(index);
  }

  public static class StreamReaderBuilder {
    private String fileName;
    private int columnIndex;
    private EncodedColumnBatch.StreamBuffer presentStream;
    private EncodedColumnBatch.StreamBuffer dataStream;
    private EncodedColumnBatch.StreamBuffer nanosStream;
    private CompressionCodec compressionCodec;
    private int bufferSize;
    private OrcProto.RowIndexEntry rowIndex;
    private OrcProto.ColumnEncoding columnEncoding;
    private boolean skipCorrupt;

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

    public StreamReaderBuilder setSecondsStream(EncodedColumnBatch.StreamBuffer dataStream) {
      this.dataStream = dataStream;
      return this;
    }

    public StreamReaderBuilder setNanosStream(EncodedColumnBatch.StreamBuffer secondaryStream) {
      this.nanosStream = secondaryStream;
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

    public StreamReaderBuilder skipCorrupt(boolean skipCorrupt) {
      this.skipCorrupt = skipCorrupt;
      return this;
    }

    public TimestampStreamReader build() throws IOException {
      InStream present = StreamUtils.createInStream(OrcProto.Stream.Kind.PRESENT.name(), fileName,
          null, bufferSize, presentStream);

      InStream data = StreamUtils.createInStream(OrcProto.Stream.Kind.DATA.name(), fileName,
          null, bufferSize, dataStream);

      InStream nanos = StreamUtils.createInStream(OrcProto.Stream.Kind.SECONDARY.name(), fileName,
          null, bufferSize, nanosStream);

      boolean isFileCompressed = compressionCodec != null;
      return new TimestampStreamReader(columnIndex, present, data, nanos,
          isFileCompressed, columnEncoding, skipCorrupt, rowIndex);
    }
  }

  public static StreamReaderBuilder builder() {
    return new StreamReaderBuilder();
  }
}
