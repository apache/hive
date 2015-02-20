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
 * Stream reader for decimal column type.
 */
public class DecimalStreamReader extends RecordReaderImpl.DecimalTreeReader {
  private boolean isFileCompressed;

  private DecimalStreamReader(int columnId, int precision, int scale, InStream presentStream,
      InStream valueStream, InStream scaleStream, boolean isFileCompressed,
      OrcProto.RowIndexEntry rowIndex, OrcProto.ColumnEncoding encoding) throws IOException {
    super(columnId, precision, scale, presentStream, valueStream, scaleStream, encoding);
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
    valueStream.seek(index);

    if (isFileCompressed) {
      index.getNext();
    }
    scaleReader.seek(index);
  }

  public static class StreamReaderBuilder {
    private String fileName;
    private int columnIndex;
    private EncodedColumnBatch.StreamBuffer presentStream;
    private EncodedColumnBatch.StreamBuffer valueStream;
    private EncodedColumnBatch.StreamBuffer scaleStream;
    private int scale;
    private int precision;
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

    public StreamReaderBuilder setPrecision(int precision) {
      this.precision = precision;
      return this;
    }

    public StreamReaderBuilder setScale(int scale) {
      this.scale = scale;
      return this;
    }

    public StreamReaderBuilder setPresentStream(EncodedColumnBatch.StreamBuffer presentStream) {
      this.presentStream = presentStream;
      return this;
    }

    public StreamReaderBuilder setValueStream(EncodedColumnBatch.StreamBuffer valueStream) {
      this.valueStream = valueStream;
      return this;
    }

    public StreamReaderBuilder setScaleStream(EncodedColumnBatch.StreamBuffer scaleStream) {
      this.scaleStream = scaleStream;
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

    public DecimalStreamReader build() throws IOException {
      InStream presentInStream = StreamUtils.createInStream(OrcProto.Stream.Kind.PRESENT.name(),
          fileName, null, bufferSize, presentStream);

      InStream valueInStream = StreamUtils.createInStream(OrcProto.Stream.Kind.DATA.name(),
          fileName, null, bufferSize, valueStream);

      InStream scaleInStream = StreamUtils.createInStream(OrcProto.Stream.Kind.SECONDARY.name(),
          fileName, null, bufferSize, scaleStream);

      boolean isFileCompressed = compressionCodec != null;
      return new DecimalStreamReader(columnIndex, precision, scale, presentInStream, valueInStream,
          scaleInStream, isFileCompressed, rowIndex, columnEncoding);
    }
  }

  public static StreamReaderBuilder builder() {
    return new StreamReaderBuilder();
  }
}
