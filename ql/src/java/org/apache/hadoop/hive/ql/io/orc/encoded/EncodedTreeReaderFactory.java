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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import org.apache.orc.impl.RunLengthByteReader;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch.ColumnStreamData;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.orc.CompressionCodec;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.SettableUncompressedStream;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.OrcProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncodedTreeReaderFactory extends TreeReaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EncodedTreeReaderFactory.class);
  /**
   * We choose to use a toy programming language, so we cannot use multiple inheritance.
   * If we could, we could have this inherit TreeReader to contain the common impl, and then
   * have e.g. SettableIntTreeReader inherit both Settable... and Int.. TreeReader-s.
   * Instead, we have a settable interface that the caller will cast to and call setBuffers.
   */
  public interface SettableTreeReader {
    void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe) throws IOException;
  }

  public static class TimestampStreamReader extends TimestampTreeReader
      implements SettableTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _secondsStream;
    private SettableUncompressedStream _nanosStream;
    private List<ColumnVector> vectors;
    private int vectorIndex = 0;

    private TimestampStreamReader(int columnId, SettableUncompressedStream present,
                                  SettableUncompressedStream data, SettableUncompressedStream nanos,
                                  boolean isFileCompressed, OrcProto.ColumnEncoding encoding,
                                  TreeReaderFactory.Context context,
                                  List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, nanos, encoding, context);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._secondsStream = data;
      this._nanosStream = nanos;
      this.vectors = vectors;
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      // Note: we assume that batchSize will be consistent with vectors passed in.
      // This is rather brittle; same in other readers.
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_secondsStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        data.seek(index);
      }

      if (_nanosStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        nanos.seek(index);
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      // The situation here and in other readers is currently as such - setBuffers is never called
      // in SerDe reader case, and SerDe reader case is the only one that uses vector-s.
      // When the readers are created with vectors, streams are actually not created at all.
      // So, if we could have a set of vectors, then set of buffers, we'd be in trouble here;
      // we may need to implement that if this scenario is ever supported.
      assert vectors == null;
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_secondsStream != null) {
        _secondsStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (_nanosStream != null) {
        _nanosStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.SECONDARY_VALUE]));
      }
    }

    public void updateTimezone(String writerTimezoneId) throws IOException {
      base_timestamp = getBaseTimestamp(writerTimezoneId);
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData nanosStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReaderFactory.Context context;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setSecondsStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setNanosStream(ColumnStreamData secondaryStream) {
        this.nanosStream = secondaryStream;
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public TimestampStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        SettableUncompressedStream nanos = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.SECONDARY.name(),
                nanosStream);

        boolean isFileCompressed = compressionCodec != null;
        return new TimestampStreamReader(columnIndex, present, data, nanos,
            isFileCompressed, columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class StringStreamReader extends StringTreeReader
      implements SettableTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private StringStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, SettableUncompressedStream length,
        SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding,
        TreeReaderFactory.Context context, List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, length, dictionary, encoding, context);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      // This string reader should simply redirect to its own seek (what other types already do).
      this.seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.getPresent().seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream != null && _dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).getReader().seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream != null && _dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getStream().seek(index);
        }

        if (_lengthStream != null && _lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getLengths().seek(index);
        }
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DICTIONARY_DATA_VALUE]));
        }
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public StringStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(),
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new StringStreamReader(columnIndex, present, data, length, dictionary,
            isFileCompressed, columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class ShortStreamReader extends ShortTreeReader implements SettableTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private ShortStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, encoding, context);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public ShortStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new ShortStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }

    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class LongStreamReader extends LongTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
    private int vectorIndex = 0;

    private LongStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, encoding, context);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReaderFactory.Context context;
      private List<ColumnVector> vectors;


      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public LongStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new LongStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }

    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class IntStreamReader extends IntTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private IntStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, encoding, context);
      this._isFileCompressed = isFileCompressed;
      this._dataStream = data;
      this._presentStream = present;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public IntStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new IntStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }

    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class FloatStreamReader extends FloatTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private FloatStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public FloatStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new FloatStreamReader(columnIndex, present, data, isFileCompressed, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class DoubleStreamReader extends DoubleTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private DoubleStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public DoubleStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        // TODO: why doesn't this use context?
        return new DoubleStreamReader(columnIndex, present, data, isFileCompressed, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class DecimalStreamReader extends DecimalTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _valueStream;
    private SettableUncompressedStream _scaleStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private DecimalStreamReader(int columnId, int precision, int scale,
        SettableUncompressedStream presentStream,
        SettableUncompressedStream valueStream, SettableUncompressedStream scaleStream,
        boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, presentStream, valueStream, scaleStream, encoding, context);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = presentStream;
      this._valueStream = valueStream;
      this._scaleStream = scaleStream;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
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
        valueStream.seek(index);
      }

      if (_scaleStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        scaleReader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_valueStream != null) {
        _valueStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (_scaleStream != null) {
        _scaleStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.SECONDARY_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData valueStream;
      private ColumnStreamData scaleStream;
      private int scale;
      private int precision;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setValueStream(ColumnStreamData valueStream) {
        this.valueStream = valueStream;
        return this;
      }

      public StreamReaderBuilder setScaleStream(ColumnStreamData scaleStream) {
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
        SettableUncompressedStream presentInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.PRESENT.name(), presentStream);

        SettableUncompressedStream valueInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DATA.name(), valueStream);

        SettableUncompressedStream scaleInStream = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.SECONDARY.name(), scaleStream);

        boolean isFileCompressed = compressionCodec != null;
        return new DecimalStreamReader(columnIndex, precision, scale, presentInStream,
            valueInStream,
            scaleInStream, isFileCompressed, columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class DateStreamReader extends DateTreeReader implements SettableTreeReader {
    private boolean isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private DateStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context, List<ColumnVector> vectors
        ) throws IOException {
      super(columnId, present, data, encoding, context);
      this.isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }

      public DateStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);


        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new DateStreamReader(columnIndex, present, data, isFileCompressed,
            columnEncoding, context, vectors);
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class CharStreamReader extends CharTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private CharStreamReader(int columnId, int maxLength,
        SettableUncompressedStream present, SettableUncompressedStream data,
        SettableUncompressedStream length, SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, maxLength, present, data, length,
          dictionary, encoding);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      // This string reader should simply redirect to its own seek (what other types already do).
      this.seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.getPresent().seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).getReader().seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getStream().seek(index);
        }

        if (_lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getLengths().seek(index);
        }
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DICTIONARY_DATA_VALUE]));
        }
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private int maxLength;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setMaxLength(int maxLength) {
        this.maxLength = maxLength;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
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

      public CharStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(),
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new CharStreamReader(columnIndex, maxLength, present, data, length,
            dictionary, isFileCompressed, columnEncoding, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class VarcharStreamReader extends VarcharTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private boolean _isDictionaryEncoding;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthStream;
    private SettableUncompressedStream _dictionaryStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private VarcharStreamReader(int columnId, int maxLength,
        SettableUncompressedStream present, SettableUncompressedStream data,
        SettableUncompressedStream length, SettableUncompressedStream dictionary,
        boolean isFileCompressed, OrcProto.ColumnEncoding encoding,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, maxLength, present, data, length,
          dictionary, encoding);
      this._isDictionaryEncoding = dictionary != null;
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthStream = length;
      this._dictionaryStream = dictionary;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      // This string reader should simply redirect to its own seek (what other types already do).
      this.seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.getPresent().seek(index);
      }

      if (_isDictionaryEncoding) {
        // DICTIONARY encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDictionaryTreeReader) reader).getReader().seek(index);
        }
      } else {
        // DIRECT encoding

        // data stream could be empty stream or already reached end of stream before present stream.
        // This can happen if all values in stream are nulls or last row group values are all null.
        if (_dataStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getStream().seek(index);
        }

        if (_lengthStream.available() > 0) {
          if (_isFileCompressed) {
            index.getNext();
          }
          ((StringDirectTreeReader) reader).getLengths().seek(index);
        }
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (!_isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
      }

      // set these streams only if the stripe is different
      if (!sameStripe && _isDictionaryEncoding) {
        if (_lengthStream != null) {
          _lengthStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
        }
        if (_dictionaryStream != null) {
          _dictionaryStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DICTIONARY_DATA_VALUE]));
        }
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private int maxLength;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData dictionaryStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setMaxLength(int maxLength) {
        this.maxLength = maxLength;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public StreamReaderBuilder setDictionaryStream(ColumnStreamData dictStream) {
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

      public VarcharStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(),
                lengthStream);

        SettableUncompressedStream dictionary = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DICTIONARY_DATA.name(), dictionaryStream);

        boolean isFileCompressed = compressionCodec != null;
        return new VarcharStreamReader(columnIndex, maxLength, present, data, length,
            dictionary, isFileCompressed, columnEncoding, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }

  }

  protected static class ByteStreamReader extends ByteTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private ByteStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public ByteStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new ByteStreamReader(columnIndex, present, data, isFileCompressed, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class BinaryStreamReader extends BinaryTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private SettableUncompressedStream _lengthsStream;
    private List<ColumnVector> vectors;
      private int vectorIndex = 0;

    private BinaryStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, SettableUncompressedStream length,
        boolean isFileCompressed,
        OrcProto.ColumnEncoding encoding, TreeReaderFactory.Context context, List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data, length, encoding, context);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this._lengthsStream = length;
      this.vectors = vectors;
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        stream.seek(index);
      }

      if (lengths != null && _lengthsStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        lengths.seek(index);
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (_lengthsStream != null) {
        _lengthsStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private List<ColumnVector> vectors;
      private TreeReaderFactory.Context context;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setLengthStream(ColumnStreamData secondaryStream) {
        this.lengthStream = secondaryStream;
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

      public StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public BinaryStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.PRESENT.name(), presentStream);

        SettableUncompressedStream data = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DATA.name(), dataStream);

        SettableUncompressedStream length = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.LENGTH.name(), lengthStream);

        boolean isFileCompressed = compressionCodec != null;
        return new BinaryStreamReader(columnIndex, present, data, length, isFileCompressed,
            columnEncoding, context, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  protected static class BooleanStreamReader extends BooleanTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;
    private List<ColumnVector> vectors;
    private int vectorIndex = 0;

    private BooleanStreamReader(int columnId, SettableUncompressedStream present,
        SettableUncompressedStream data, boolean isFileCompressed,
        List<ColumnVector> vectors) throws IOException {
      super(columnId, present, data);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = data;
      this.vectors = vectors;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      if (vectors != null) return;
      if (present != null) {
        if (_isFileCompressed) {
          index.getNext();
        }
        present.seek(index);
      }

      // data stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          index.getNext();
        }
        reader.seek(index);
      }
    }

    @Override
    public void nextVector(
        ColumnVector previousVector, boolean[] isNull, int batchSize) throws IOException {
      if (vectors == null) {
        super.nextVector(previousVector, isNull, batchSize);
        return;
      }
      vectors.get(vectorIndex++).shallowCopyTo(previousVector);
      if (vectorIndex == vectors.size()) {
        vectors = null;
      }
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      assert vectors == null; // See the comment in TimestampStreamReader.setBuffers.
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private List<ColumnVector> vectors;

      public StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public BooleanStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream data = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.DATA.name(),
                dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new BooleanStreamReader(columnIndex, present, data, isFileCompressed, vectors);
      }

      public StreamReaderBuilder setVectors(List<ColumnVector> vectors) {
        this.vectors = vectors;
        return this;
      }
    }

    public static StreamReaderBuilder builder() {
      return new StreamReaderBuilder();
    }
  }

  public static StructTreeReader createRootTreeReader(TypeDescription schema,
       List<OrcProto.ColumnEncoding> encodings, OrcEncodedColumnBatch batch,
       CompressionCodec codec, TreeReaderFactory.Context context, int[] columnMapping)
           throws IOException {
    if (schema.getCategory() != Category.STRUCT) {
      throw new AssertionError("Schema is not a struct: " + schema);
    }
    // Some child types may be excluded. Note that this can only happen at root level.
    List<TypeDescription> children = schema.getChildren();
    int childCount = children.size(), includedCount = 0;
    for (int childIx = 0; childIx < childCount; ++childIx) {
      int batchColIx = children.get(childIx).getId();
      if (!batch.hasData(batchColIx) && !batch.hasVectors(batchColIx)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Column at " + childIx + " " + children.get(childIx).getId()
              + ":" + children.get(childIx).toString() + " has no data");
        }
        continue;
      }
      ++includedCount;
    }
    TreeReader[] childReaders = new TreeReader[includedCount];
    for (int schemaChildIx = 0, inclChildIx = -1; schemaChildIx < childCount; ++schemaChildIx) {
      int batchColIx = children.get(schemaChildIx).getId();
      if (!batch.hasData(batchColIx) && !batch.hasVectors(batchColIx)) continue;
      childReaders[++inclChildIx] = createEncodedTreeReader(
          schema.getChildren().get(schemaChildIx), encodings, batch, codec, context);
      columnMapping[inclChildIx] = schemaChildIx;
    }
    return StructStreamReader.builder()
        .setColumnIndex(0)
        .setCompressionCodec(codec)
        .setColumnEncoding(encodings.get(0))
        .setChildReaders(childReaders)
        .setContext(context)
        .build();
  }


  private static TreeReader createEncodedTreeReader(TypeDescription schema,
      List<OrcProto.ColumnEncoding> encodings, OrcEncodedColumnBatch batch,
      CompressionCodec codec, TreeReaderFactory.Context context) throws IOException {
    int columnIndex = schema.getId();
    ColumnStreamData[] streamBuffers = null;
    List<ColumnVector> vectors = null;
    if (batch.hasData(columnIndex)) {
      streamBuffers = batch.getColumnData(columnIndex);
    } else if (batch.hasVectors(columnIndex)) {
      vectors = batch.getColumnVectors(columnIndex);
    } else {
      throw new AssertionError("Batch has no data for " + columnIndex + ": " + batch);
    }

    // EncodedColumnBatch is already decompressed, we don't really need to pass codec.
    // But we need to know if the original data is compressed or not. This is used to skip
    // positions in row index properly. If the file is originally compressed,
    // then 1st position (compressed offset) in row index should be skipped to get
    // uncompressed offset, else 1st position should not be skipped.
    // TODO: there should be a better way to do this, code just needs to be modified
    OrcProto.ColumnEncoding columnEncoding = encodings.get(columnIndex);

    // stream buffers are arranged in enum order of stream kind
    ColumnStreamData present = null, data = null, dictionary = null,
        lengths = null, secondary = null;
    if (streamBuffers != null) {
      present = streamBuffers[OrcProto.Stream.Kind.PRESENT_VALUE];
      data = streamBuffers[OrcProto.Stream.Kind.DATA_VALUE];
      dictionary = streamBuffers[OrcProto.Stream.Kind.DICTIONARY_DATA_VALUE];
      lengths = streamBuffers[OrcProto.Stream.Kind.LENGTH_VALUE];
      secondary = streamBuffers[OrcProto.Stream.Kind.SECONDARY_VALUE];
    }


    if (LOG.isDebugEnabled()) {
      LOG.debug("columnIndex: {} columnType: {} streamBuffers.length: {} vectors: {} columnEncoding: {}" +
          " present: {} data: {} dictionary: {} lengths: {} secondary: {} tz: {}",
          columnIndex, schema, streamBuffers == null ? 0 : streamBuffers.length,
          vectors == null ? 0 : vectors.size(), columnEncoding, present != null,
          data, dictionary != null, lengths != null, secondary != null,
          context.getWriterTimezone());
    }
    // TODO: get rid of the builders - they serve no purpose... just call ctors directly.
    switch (schema.getCategory()) {
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case STRING:
      case DECIMAL:
      case TIMESTAMP:
      case DATE:
        return getPrimitiveTreeReader(columnIndex, schema, codec, columnEncoding,
            present, data, dictionary, lengths, secondary, context, vectors);
      case LIST:
        assert vectors == null; // Not currently supported.
        TypeDescription elementType = schema.getChildren().get(0);
        TreeReader elementReader = createEncodedTreeReader(
            elementType, encodings, batch, codec, context);
        return ListStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setColumnEncoding(columnEncoding)
            .setCompressionCodec(codec)
            .setPresentStream(present)
            .setLengthStream(lengths)
            .setElementReader(elementReader)
            .setContext(context)
            .build();
      case MAP:
        assert vectors == null; // Not currently supported.
        TypeDescription keyType = schema.getChildren().get(0);
        TypeDescription valueType = schema.getChildren().get(1);
        TreeReader keyReader = createEncodedTreeReader(
            keyType, encodings, batch, codec, context);
        TreeReader valueReader = createEncodedTreeReader(
            valueType, encodings, batch, codec, context);
        return MapStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setColumnEncoding(columnEncoding)
            .setCompressionCodec(codec)
            .setPresentStream(present)
            .setLengthStream(lengths)
            .setKeyReader(keyReader)
            .setValueReader(valueReader)
            .setContext(context)
            .build();
      case STRUCT: {
        assert vectors == null; // Not currently supported.
        int childCount = schema.getChildren().size();
        TreeReader[] childReaders = new TreeReader[childCount];
        for (int i = 0; i < childCount; i++) {
          TypeDescription childType = schema.getChildren().get(i);
          childReaders[i] = createEncodedTreeReader(
              childType, encodings, batch, codec, context);
        }
        return StructStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setPresentStream(present)
            .setChildReaders(childReaders)
            .setContext(context)
            .build();
      }
      case UNION: {
        assert vectors == null; // Not currently supported.
        int childCount = schema.getChildren().size();
        TreeReader[] childReaders = new TreeReader[childCount];
        for (int i = 0; i < childCount; i++) {
          TypeDescription childType = schema.getChildren().get(i);
          childReaders[i] = createEncodedTreeReader(
              childType, encodings, batch, codec, context);
        }
        return UnionStreamReader.builder()
              .setColumnIndex(columnIndex)
              .setCompressionCodec(codec)
              .setColumnEncoding(columnEncoding)
              .setPresentStream(present)
              .setDataStream(data)
              .setChildReaders(childReaders)
              .setContext(context)
              .build();
      }
      default:
        throw new UnsupportedOperationException("Data type not supported: " + schema);
    }
  }

  private static TreeReader getPrimitiveTreeReader(final int columnIndex,
      TypeDescription columnType, CompressionCodec codec, OrcProto.ColumnEncoding columnEncoding,
      ColumnStreamData present, ColumnStreamData data, ColumnStreamData dictionary,
      ColumnStreamData lengths, ColumnStreamData secondary, TreeReaderFactory.Context context,
      List<ColumnVector> vectors) throws IOException {
    switch (columnType.getCategory()) {
      case BINARY:
        return BinaryStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setLengthStream(lengths)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case BOOLEAN:
        return BooleanStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setVectors(vectors)
            .build();
      case BYTE:
        return ByteStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setVectors(vectors)
            .build();
      case SHORT:
        return ShortStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case INT:
        return IntStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case LONG:
        return LongStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case FLOAT:
        return FloatStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setVectors(vectors)
            .build();
      case DOUBLE:
        return DoubleStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setVectors(vectors)
            .build();
      case CHAR:
        return CharStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setMaxLength(columnType.getMaxLength())
            .setPresentStream(present)
            .setDataStream(data)
            .setLengthStream(lengths)
            .setDictionaryStream(dictionary)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .build();
      case VARCHAR:
        return VarcharStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setMaxLength(columnType.getMaxLength())
            .setPresentStream(present)
            .setDataStream(data)
            .setLengthStream(lengths)
            .setDictionaryStream(dictionary)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .build();
      case STRING:
        return StringStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setLengthStream(lengths)
            .setDictionaryStream(dictionary)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .build();
      case DECIMAL:
        return DecimalStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPrecision(columnType.getPrecision())
            .setScale(columnType.getScale())
            .setPresentStream(present)
            .setValueStream(data)
            .setScaleStream(secondary)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case TIMESTAMP:
        return TimestampStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setSecondsStream(data)
            .setNanosStream(secondary)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
      case DATE:
        return DateStreamReader.builder()
            .setColumnIndex(columnIndex)
            .setPresentStream(present)
            .setDataStream(data)
            .setCompressionCodec(codec)
            .setColumnEncoding(columnEncoding)
            .setVectors(vectors)
            .setContext(context)
            .build();
    default:
      throw new AssertionError("Not a primitive category: " + columnType.getCategory());
    }
  }

  protected static class ListStreamReader extends ListTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _lengthStream;

    public ListStreamReader(final int columnIndex,
        final SettableUncompressedStream present, final SettableUncompressedStream lengthStream,
        final OrcProto.ColumnEncoding columnEncoding, final boolean isFileCompressed,
        final TreeReader elementReader,
                            TreeReaderFactory.Context context) throws IOException {
      super(columnIndex, present, context, lengthStream, columnEncoding, elementReader);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._lengthStream = lengthStream;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      PositionProvider ownIndex = index[columnId];
      if (present != null) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        present.seek(ownIndex);
      }

      // lengths stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_lengthStream.available() > 0) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        lengths.seek(ownIndex);
        elementReader.seek(index);
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      // Only our parent class can call this.
      throw new IOException("Should never be called");
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_lengthStream != null) {
        _lengthStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
      }

      if (elementReader != null) {
        ((SettableTreeReader) elementReader).setBuffers(batch, sameStripe);
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReader elementReader;
      private TreeReaderFactory.Context context;

      public ListStreamReader.StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setElementReader(TreeReader elementReader) {
        this.elementReader = elementReader;
        return this;
      }

      public ListStreamReader.StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public ListStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(),
                lengthStream);

        boolean isFileCompressed = compressionCodec != null;
        return new ListStreamReader(columnIndex, present, length, columnEncoding, isFileCompressed,
            elementReader, context);
      }
    }

    public static ListStreamReader.StreamReaderBuilder builder() {
      return new ListStreamReader.StreamReaderBuilder();
    }
  }

  protected static class MapStreamReader extends MapTreeReader implements SettableTreeReader{
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _lengthStream;

    public MapStreamReader(final int columnIndex,
        final SettableUncompressedStream present, final SettableUncompressedStream lengthStream,
        final OrcProto.ColumnEncoding columnEncoding, final boolean isFileCompressed,
        final TreeReader keyReader, final TreeReader valueReader,
                           TreeReaderFactory.Context context) throws IOException {
      super(columnIndex, present, context, lengthStream, columnEncoding, keyReader, valueReader);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._lengthStream = lengthStream;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      // We are not calling super.seek since we handle the present stream differently.
      PositionProvider ownIndex = index[columnId];
      if (present != null) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        present.seek(ownIndex);
      }

      // lengths stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_lengthStream.available() > 0) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        lengths.seek(ownIndex);
        keyReader.seek(index);
        valueReader.seek(index);
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      // Only our parent class can call this.
      throw new IOException("Should never be called");
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
     ColumnStreamData[] streamsData = batch.getColumnData(columnId);
     if (_presentStream != null) {
        _presentStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_lengthStream != null) {
        _lengthStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.LENGTH_VALUE]));
      }

      if (keyReader != null) {
        ((SettableTreeReader) keyReader).setBuffers(batch, sameStripe);
      }

      if (valueReader != null) {
        ((SettableTreeReader) valueReader).setBuffers(batch, sameStripe);
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData lengthStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReader keyReader;
      private TreeReader valueReader;
      private TreeReaderFactory.Context context;

      public MapStreamReader.StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setLengthStream(ColumnStreamData lengthStream) {
        this.lengthStream = lengthStream;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setKeyReader(TreeReader keyReader) {
        this.keyReader = keyReader;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setValueReader(TreeReader valueReader) {
        this.valueReader = valueReader;
        return this;
      }

      public MapStreamReader.StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public MapStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        SettableUncompressedStream length = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.LENGTH.name(),
                lengthStream);

        boolean isFileCompressed = compressionCodec != null;
        return new MapStreamReader(columnIndex, present, length, columnEncoding, isFileCompressed,
            keyReader, valueReader, context);
      }
    }

    public static MapStreamReader.StreamReaderBuilder builder() {
      return new MapStreamReader.StreamReaderBuilder();
    }
  }

  protected static class StructStreamReader extends StructTreeReader
      implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;

    public StructStreamReader(final int columnIndex,
        final SettableUncompressedStream present,
        final OrcProto.ColumnEncoding columnEncoding, final boolean isFileCompressed,
        final TreeReader[] childReaders, TreeReaderFactory.Context context) throws IOException {
      super(columnIndex, present, context, columnEncoding, childReaders);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      PositionProvider ownIndex = index[columnId];
      if (present != null) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        present.seek(ownIndex);
      }
      if (fields != null) {
        for (TreeReader child : fields) {
          child.seek(index);
        }
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      // Only our parent class can call this.
      throw new IOException("Should never be called");
    }


    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (fields != null) {
        for (TreeReader child : fields) {
          ((SettableTreeReader) child).setBuffers(batch, sameStripe);
        }
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReader[] childReaders;
      private TreeReaderFactory.Context context;

      public StructStreamReader.StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public StructStreamReader.StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public StructStreamReader.StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public StructStreamReader.StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public StructStreamReader.StreamReaderBuilder setChildReaders(TreeReader[] childReaders) {
        this.childReaders = childReaders;
        return this;
      }

      public StructStreamReader.StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public StructStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils
            .createSettableUncompressedStream(OrcProto.Stream.Kind.PRESENT.name(),
                presentStream);

        boolean isFileCompressed = compressionCodec != null;
        return new StructStreamReader(columnIndex, present, columnEncoding, isFileCompressed,
            childReaders, context);
      }
    }

    public static StructStreamReader.StreamReaderBuilder builder() {
      return new StructStreamReader.StreamReaderBuilder();
    }
  }

  protected static class UnionStreamReader extends UnionTreeReader implements SettableTreeReader {
    private boolean _isFileCompressed;
    private SettableUncompressedStream _presentStream;
    private SettableUncompressedStream _dataStream;

    public UnionStreamReader(final int columnIndex,
        final SettableUncompressedStream present, final SettableUncompressedStream dataStream,
        final OrcProto.ColumnEncoding columnEncoding, final boolean isFileCompressed,
        final TreeReader[] childReaders, TreeReaderFactory.Context context) throws IOException {
      super(columnIndex, present, context, columnEncoding, childReaders);
      this._isFileCompressed = isFileCompressed;
      this._presentStream = present;
      this._dataStream = dataStream;
      // Note: other parent readers init everything in ctor, but union does it in startStripe.
      this.tags = new RunLengthByteReader(dataStream);
    }

    @Override
    public void seek(PositionProvider[] index) throws IOException {
      PositionProvider ownIndex = index[columnId];
      if (present != null) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        present.seek(ownIndex);
      }

      // lengths stream could be empty stream or already reached end of stream before present stream.
      // This can happen if all values in stream are nulls or last row group values are all null.
      if (_dataStream.available() > 0) {
        if (_isFileCompressed) {
          ownIndex.getNext();
        }
        tags.seek(ownIndex);
        if (fields != null) {
          for (TreeReader child : fields) {
            child.seek(index);
          }
        }
      }
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      // Only our parent class can call this.
      throw new IOException("Should never be called");
    }

    @Override
    public void setBuffers(EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe)
        throws IOException {
      ColumnStreamData[] streamsData = batch.getColumnData(columnId);
      if (_presentStream != null) {
        _presentStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.PRESENT_VALUE]));
      }
      if (_dataStream != null) {
        _dataStream.setBuffers(
            StreamUtils.createDiskRangeInfo(streamsData[OrcProto.Stream.Kind.DATA_VALUE]));
      }
      if (fields != null) {
        for (TreeReader child : fields) {
          ((SettableTreeReader) child).setBuffers(batch, sameStripe);
        }
      }
    }

    public static class StreamReaderBuilder {
      private int columnIndex;
      private ColumnStreamData presentStream;
      private ColumnStreamData dataStream;
      private CompressionCodec compressionCodec;
      private OrcProto.ColumnEncoding columnEncoding;
      private TreeReader[] childReaders;
      private TreeReaderFactory.Context context;

      public UnionStreamReader.StreamReaderBuilder setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setDataStream(ColumnStreamData dataStream) {
        this.dataStream = dataStream;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setPresentStream(ColumnStreamData presentStream) {
        this.presentStream = presentStream;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setColumnEncoding(OrcProto.ColumnEncoding encoding) {
        this.columnEncoding = encoding;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setCompressionCodec(CompressionCodec compressionCodec) {
        this.compressionCodec = compressionCodec;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setChildReaders(TreeReader[] childReaders) {
        this.childReaders = childReaders;
        return this;
      }

      public UnionStreamReader.StreamReaderBuilder setContext(TreeReaderFactory.Context context) {
        this.context = context;
        return this;
      }

      public UnionStreamReader build() throws IOException {
        SettableUncompressedStream present = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.PRESENT.name(), presentStream);

        SettableUncompressedStream data = StreamUtils.createSettableUncompressedStream(
            OrcProto.Stream.Kind.DATA.name(), dataStream);

        boolean isFileCompressed = compressionCodec != null;
        return new UnionStreamReader(columnIndex, present, data,
            columnEncoding, isFileCompressed, childReaders, context);
      }
    }

    public static UnionStreamReader.StreamReaderBuilder builder() {
      return new UnionStreamReader.StreamReaderBuilder();
    }
  }
}
