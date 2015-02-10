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
import org.apache.hadoop.hive.ql.io.orc.BitFieldReader;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;

import com.google.common.base.Preconditions;

/**
 *
 */
public abstract class BaseColumnStream implements ColumnStream {
  protected int columnId;
  protected EncodedColumnBatch.StreamBuffer nullStream;
  protected InStream inStream;
  protected BitFieldReader bitFieldReader;
  protected boolean isFileCompressed;

  public BaseColumnStream(String file, int columnId, EncodedColumnBatch.StreamBuffer present,
      CompressionCodec codec, int bufferSize) throws IOException {
    Preconditions.checkArgument(columnId >= 0, "ColumnId cannot be negative");
    this.columnId = columnId;
    if (present != null) {
      this.nullStream = present;
      isFileCompressed = codec != null;
      // pass null for codec as the stream is decompressed
      this.inStream = StreamUtils.createInStream("PRESENT", file, null, bufferSize, present);
      this.bitFieldReader = present == null ? null : new BitFieldReader(inStream, 1);
    } else {
      this.nullStream = null;
      this.inStream = null;
      this.bitFieldReader = null;
    }
  }

  public void positionReaders(PositionProvider positionProvider) throws IOException {
    if (bitFieldReader != null) {
      // stream is uncompressed and if file is compressed then skip 1st position in index
      if (isFileCompressed) {
        positionProvider.getNext();
      }
      bitFieldReader.seek(positionProvider);
    }
  }

  @Override
  public void close() throws IOException {
    if (nullStream != null && inStream != null) {
      nullStream.decRef();
      nullStream = null;
      inStream.close();
    }
  }

  @Override
  public ColumnVector nextVector(ColumnVector previousVector, int batchSize) throws IOException {
    ColumnVector result = (ColumnVector) previousVector;
    if (bitFieldReader != null) {
      // Set noNulls and isNull vector of the ColumnVector based on
      // present stream
      result.noNulls = true;
      for (int i = 0; i < batchSize; i++) {
        result.isNull[i] = (bitFieldReader.next() != 1);
        if (result.noNulls && result.isNull[i]) {
          result.noNulls = false;
        }
      }
    } else {
      // There is not present stream, this means that all the values are
      // present.
      result.noNulls = true;
      for (int i = 0; i < batchSize; i++) {
        result.isNull[i] = false;
      }
    }
    return previousVector;
  }
}
