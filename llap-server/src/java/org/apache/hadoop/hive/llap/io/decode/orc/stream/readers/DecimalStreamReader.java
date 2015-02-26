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
import java.util.List;

import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.SettableUncompressedStream;
import org.apache.hadoop.hive.llap.io.decode.orc.stream.StreamUtils;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

import com.google.common.collect.Lists;

/**
 * Stream reader for decimal column type.
 */
public class DecimalStreamReader extends RecordReaderImpl.DecimalTreeReader {
  private boolean _isFileCompressed;
  private SettableUncompressedStream _presentStream;
  private SettableUncompressedStream _valueStream;
  private SettableUncompressedStream _scaleStream;

  private DecimalStreamReader(int columnId, int precision, int scale, SettableUncompressedStream presentStream,
      SettableUncompressedStream valueStream, SettableUncompressedStream scaleStream, boolean isFileCompressed,
      OrcProto.ColumnEncoding encoding) throws IOException {
    super(columnId, precision, scale, presentStream, valueStream, scaleStream, encoding);
    this._isFileCompressed = isFileCompressed;
    this._presentStream = presentStream;
    this._valueStream = valueStream;
    this._scaleStream = scaleStream;
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    if (present != null) {
      if (_isFileCompressed) {
        index.getNext();
      }
      present.seek(index);
    }

    // data stream could be empty stream or already reached end of stream before present stream.
    // This can happen if all values in stream are nulls or last row group values are all null.
    if (_valueStream.available() > 0) {
      if (_isFileCompressed) {
        index.getNext();
      }
      value.seek(index);
    }

    if (_scaleStream.available() > 0) {
      if (_isFileCompressed) {
        index.getNext();
      }
      scaleReader.seek(index);
    }
  }

  public void setBuffers(EncodedColumnBatch.StreamBuffer presentStreamBuffer,
      EncodedColumnBatch.StreamBuffer valueStreamBuffer,
      EncodedColumnBatch.StreamBuffer scaleStreamBuffer) {
    long length;
    if (_presentStream != null) {
      List<DiskRange> presentDiskRanges = Lists.newArrayList();
      length = StreamUtils.createDiskRanges(presentStreamBuffer, presentDiskRanges);
      _presentStream.setBuffers(presentDiskRanges, length);
    }
    if (_valueStream != null) {
      List<DiskRange> valueDiskRanges = Lists.newArrayList();
      length = StreamUtils.createDiskRanges(valueStreamBuffer, valueDiskRanges);
      _valueStream.setBuffers(valueDiskRanges, length);
    }
    if (_scaleStream != null) {
      List<DiskRange> scaleDiskRanges = Lists.newArrayList();
      length = StreamUtils.createDiskRanges(scaleStreamBuffer, scaleDiskRanges);
      _scaleStream.setBuffers(scaleDiskRanges, length);
    }
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

    public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
      this.columnEncoding = encoding;
      return this;
    }

    public DecimalStreamReader build() throws IOException {
      SettableUncompressedStream presentInStream = StreamUtils.createLlapInStream(
          OrcProto.Stream.Kind.PRESENT.name(), fileName, presentStream);

      SettableUncompressedStream valueInStream = StreamUtils.createLlapInStream(
          OrcProto.Stream.Kind.DATA.name(), fileName, valueStream);

      SettableUncompressedStream scaleInStream = StreamUtils.createLlapInStream(
          OrcProto.Stream.Kind.SECONDARY.name(), fileName, scaleStream);

      boolean isFileCompressed = compressionCodec != null;
      return new DecimalStreamReader(columnIndex, precision, scale, presentInStream, valueInStream,
          scaleInStream, isFileCompressed, columnEncoding);
    }
  }

  public static StreamReaderBuilder builder() {
    return new StreamReaderBuilder();
  }
}
