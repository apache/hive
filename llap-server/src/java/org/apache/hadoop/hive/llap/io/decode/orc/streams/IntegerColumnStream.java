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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.IntegerReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;

import com.google.common.base.Preconditions;

/**
 * Integer stream reader.
 */
public class IntegerColumnStream extends BaseColumnStream {
  private IntegerReader integerReader;
  private InStream dataStream;

  public IntegerColumnStream(String file, int colIx, EncodedColumnBatch.StreamBuffer present,
      EncodedColumnBatch.StreamBuffer data, OrcProto.ColumnEncoding columnEncoding,
      CompressionCodec codec, int bufferSize, OrcProto.RowIndexEntry rowIndex)
      throws IOException {
    super(file, colIx, present, codec, bufferSize);

    Preconditions.checkNotNull(data, "DATA stream buffer cannot be null");

    // pass null for codec as stream is already decompressed
    this.dataStream = StreamUtils.createInStream("DATA", file, null, bufferSize, data);
    this.integerReader = StreamUtils.createIntegerReader(columnEncoding.getKind(), dataStream, true);

    // position the readers based on the specified row index
    PositionProvider positionProvider = new RecordReaderImpl.PositionProviderImpl(rowIndex);
    positionReaders(positionProvider);
  }

  public void positionReaders(PositionProvider positionProvider) throws IOException {

    // position the present stream
    super.positionReaders(positionProvider);

    // stream is uncompressed and if file is compressed then skip 1st position in index
    if (isFileCompressed) {
      positionProvider.getNext();
    }
    integerReader.seek(positionProvider);
  }

  @Override
  public ColumnVector nextVector(ColumnVector previousVector, int batchSize) throws IOException {
    LongColumnVector result = null;
    if (previousVector == null) {
      result = new LongColumnVector();
    } else {
      result = (LongColumnVector) previousVector;
    }

    // Read present/isNull stream
    super.nextVector(result, batchSize);

    // Read value entries based on isNull entries
    integerReader.nextVector(result, batchSize);
    return result;
  }
}
