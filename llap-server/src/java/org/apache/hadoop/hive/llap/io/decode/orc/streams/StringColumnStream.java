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
package org.apache.hadoop.hive.llap.io.decode.orc.streams;

import java.io.IOException;

import org.apache.hadoop.hive.llap.io.api.EncodedColumnBatch;
import org.apache.hadoop.hive.llap.io.decode.orc.readers.DictionaryStringReader;
import org.apache.hadoop.hive.llap.io.decode.orc.readers.DirectStringReader;
import org.apache.hadoop.hive.llap.io.decode.orc.readers.StringReader;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.IntegerReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

import com.google.common.base.Preconditions;

/**
 * String column stream reader.
 */
public class StringColumnStream extends BaseColumnStream {
  private IntegerReader lengthReader;
  private StringReader stringReader;
  private IntegerReader dataReader;
  private InStream dictionaryStream;
  private InStream lengthStream;
  private InStream dataStream;
  private OrcProto.ColumnEncoding.Kind kind;

  public StringColumnStream(String file, int colIx, EncodedColumnBatch.StreamBuffer present,
      EncodedColumnBatch.StreamBuffer data, EncodedColumnBatch.StreamBuffer dictionary,
      EncodedColumnBatch.StreamBuffer lengths, OrcProto.ColumnEncoding columnEncoding,
      CompressionCodec codec, int bufferSize, OrcProto.RowIndexEntry rowIndex)
      throws IOException {
    super(file, colIx, present, codec, bufferSize);

    // preconditions check
    Preconditions.checkNotNull(data, "DATA stream buffer cannot be null");
    Preconditions.checkNotNull(columnEncoding, "ColumnEncoding cannot be null");
    Preconditions.checkNotNull(lengths, "ColumnEncoding is " + columnEncoding + "." +
        " Length stream cannot be null");

    this.dataStream = StreamUtils.createInStream("DATA", file, null, bufferSize, data);
    this.dataReader = StreamUtils.createIntegerReader(kind, dataStream, false);

    this.lengthStream = StreamUtils.createInStream("LENGTH", file, null, bufferSize, lengths);
    this.lengthReader = StreamUtils.createIntegerReader(kind, lengthStream, false);

    this.kind = columnEncoding.getKind();
    if (kind.equals(OrcProto.ColumnEncoding.Kind.DICTIONARY) ||
        kind.equals(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2)) {
      Preconditions.checkNotNull(dictionary, "ColumnEncoding is " + columnEncoding + "." +
          " Dictionary stream cannot be null");
      this.dictionaryStream = StreamUtils.createInStream(kind.toString(), file, null, bufferSize,
          dictionary);
    }

    if (kind.equals(OrcProto.ColumnEncoding.Kind.DIRECT) ||
        kind.equals(OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
      this.stringReader = new DirectStringReader(lengthReader, dataStream);
    } else {
      this.stringReader = new DictionaryStringReader(lengthReader, dataReader, dictionaryStream,
          columnEncoding.getDictionarySize());
    }

    // position the readers based on the specified row index
    PositionProvider positionProvider = new RecordReaderImpl.PositionProviderImpl(rowIndex);
    positionReaders(positionProvider);
  }

  public void positionReaders(PositionProvider positionProvider) throws IOException {
    super.positionReaders(positionProvider);

    // stream is uncompressed and if file is compressed then skip 1st position in index
    if (isFileCompressed) {
      positionProvider.getNext();
    }
    dataReader.seek(positionProvider);

    if (kind.equals(OrcProto.ColumnEncoding.Kind.DIRECT) || kind.equals(
        OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
      if (isFileCompressed) {
        positionProvider.getNext();
      }
      lengthReader.seek(positionProvider);
    }
  }

  @Override
  public ColumnVector nextVector(ColumnVector previousVector, int batchSize) throws IOException {
    super.nextVector(previousVector, batchSize);
    return stringReader.nextVector(previousVector, batchSize);
  }
}
