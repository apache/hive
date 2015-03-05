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
 * Stream reader for char and varchar column types.
 */
public class CharacterStreamReader extends RecordReaderImpl.StringTreeReader {
  private boolean _isFileCompressed;
  private boolean _isDictionaryEncoding;
  private SettableUncompressedStream _presentStream;
  private SettableUncompressedStream _dataStream;
  private SettableUncompressedStream _lengthStream;
  private SettableUncompressedStream _dictionaryStream;

  private CharacterStreamReader(int columnId, int maxLength, OrcProto.Type charType,
      SettableUncompressedStream present, SettableUncompressedStream data, SettableUncompressedStream length, SettableUncompressedStream dictionary,
      boolean isFileCompressed, OrcProto.ColumnEncoding encoding) throws IOException {
    super(columnId);
    this._isDictionaryEncoding = dictionary != null;
    if (charType.getKind() == OrcProto.Type.Kind.CHAR) {
      reader = new RecordReaderImpl.CharTreeReader(columnId, maxLength, present, data, length,
          dictionary, encoding);
    } else if (charType.getKind() == OrcProto.Type.Kind.VARCHAR) {
      reader = new RecordReaderImpl.VarcharTreeReader(columnId, maxLength, present, data,
          length, dictionary, encoding);
    } else {
      throw new IOException("Unknown character type " + charType + ". Expected CHAR or VARCHAR.");
    }
    this._isFileCompressed = isFileCompressed;
    this._presentStream = present;
    this._dataStream = data;
    this._lengthStream = length;
    this._dictionaryStream = dictionary;
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    if (present != null) {
      if (_isFileCompressed) {
        index.getNext();
      }
      reader.present.seek(index);
    }

    if (_isDictionaryEncoding) {
      // DICTIONARY encoding

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        ((RecordReaderImpl.StringDictionaryTreeReader) reader).reader.seek(index);
      }
    } else {
      // DIRECT encoding

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        ((RecordReaderImpl.StringDirectTreeReader) reader).stream.seek(index);
      }

      if (_lengthStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        ((RecordReaderImpl.StringDirectTreeReader) reader).lengths.seek(index);
      }
    }
  }

  public void setBuffers(EncodedColumnBatch.StreamBuffer presentStreamBuffer,
      EncodedColumnBatch.StreamBuffer dataStreamBuffer,
      EncodedColumnBatch.StreamBuffer lengthStreamBuffer,
      EncodedColumnBatch.StreamBuffer dictionaryStreamBuffer,
      boolean sameStripe) {
    long length;
    if (_presentStream != null) {
      List<DiskRange> presentDiskRanges = Lists.newArrayList();
      length = StreamUtils.createDiskRanges(presentStreamBuffer, presentDiskRanges);
      _presentStream.setBuffers(presentDiskRanges, length);
    }
    if (_dataStream != null) {
      List<DiskRange> dataDiskRanges = Lists.newArrayList();
      length = StreamUtils.createDiskRanges(dataStreamBuffer, dataDiskRanges);
      _dataStream.setBuffers(dataDiskRanges, length);
    }
    if (!_isDictionaryEncoding) {
      if (_lengthStream != null) {
        List<DiskRange> lengthDiskRanges = Lists.newArrayList();
        length = StreamUtils.createDiskRanges(lengthStreamBuffer, lengthDiskRanges);
        _lengthStream.setBuffers(lengthDiskRanges, length);
      }
    }

    // set these streams only if the stripe is different
    if (!sameStripe && _isDictionaryEncoding) {
      if (_lengthStream != null) {
        List<DiskRange> lengthDiskRanges = Lists.newArrayList();
        length = StreamUtils.createDiskRanges(lengthStreamBuffer, lengthDiskRanges);
        _lengthStream.setBuffers(lengthDiskRanges, length);
      }
      if (_dictionaryStream != null) {
        List<DiskRange> dictionaryDiskRanges = Lists.newArrayList();
        length = StreamUtils.createDiskRanges(dictionaryStreamBuffer, dictionaryDiskRanges);
        _dictionaryStream.setBuffers(dictionaryDiskRanges, length);
      }
    }
  }

  public static class StreamReaderBuilder {
    private Long fileId;
    private int columnIndex;
    private int maxLength;
    private OrcProto.Type charType;
    private EncodedColumnBatch.StreamBuffer presentStream;
    private EncodedColumnBatch.StreamBuffer dataStream;
    private EncodedColumnBatch.StreamBuffer dictionaryStream;
    private EncodedColumnBatch.StreamBuffer lengthStream;
    private CompressionCodec compressionCodec;
    private OrcProto.ColumnEncoding columnEncoding;

    public StreamReaderBuilder setFileId(Long fileId) {
      this.fileId = fileId;
      return this;
    }

    public StreamReaderBuilder setColumnIndex(int columnIndex) {
      this.columnIndex = columnIndex;
      return this;
    }

    public StreamReaderBuilder setMaxLength(int maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public StreamReaderBuilder setCharacterType(OrcProto.Type charType) {
      this.charType = charType;
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

    public StreamReaderBuilder setLengthStream(EncodedColumnBatch.StreamBuffer lengthStream) {
      this.lengthStream = lengthStream;
      return this;
    }

    public StreamReaderBuilder setDictionaryStream(EncodedColumnBatch.StreamBuffer dictStream) {
      this.dictionaryStream = dictStream;
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

    public CharacterStreamReader build() throws IOException {
      SettableUncompressedStream present = StreamUtils.createLlapInStream(OrcProto.Stream.Kind.PRESENT.name(),
          fileId, presentStream);

      SettableUncompressedStream data = StreamUtils.createLlapInStream(OrcProto.Stream.Kind.DATA.name(), fileId, 
          dataStream);

      SettableUncompressedStream length = StreamUtils.createLlapInStream(OrcProto.Stream.Kind.LENGTH.name(), fileId, 
          lengthStream);

      SettableUncompressedStream dictionary = StreamUtils.createLlapInStream(
          OrcProto.Stream.Kind.DICTIONARY_DATA.name(), fileId, dictionaryStream);

      boolean isFileCompressed = compressionCodec != null;
      return new CharacterStreamReader(columnIndex, maxLength, charType, present, data, length,
          dictionary, isFileCompressed, columnEncoding);
    }
  }

  public static StreamReaderBuilder builder() {
    return new StreamReaderBuilder();
  }

}
