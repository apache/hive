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
package org.apache.hadoop.hive.llap.io.decode.orc.readers;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.IntegerReader;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class DirectStringReader implements StringReader {
  private IntegerReader lengths;
  private InStream data;
  private final LongColumnVector scratchlcv;

  public DirectStringReader(IntegerReader lengths, InStream data) {
    this.lengths = lengths;
    this.data = data;
    this.scratchlcv = new LongColumnVector();
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    throw new UnsupportedOperationException("Operation unsupported yet.");
  }

  @Override
  public void skip(long numValues) throws IOException {
    throw new UnsupportedOperationException("Operation unsupported yet.");
  }

  @Override
  public boolean hasNext() throws IOException {
    return lengths.hasNext();
  }

  @Override
  public Text next() throws IOException {
    int len = (int) lengths.next();
    int offset = 0;
    byte[] bytes = new byte[len];
    while (len > 0) {
      int written = data.read(bytes, offset, len);
      if (written < 0) {
        throw new EOFException("Can't finish byte read from " + data);
      }
      len -= written;
      offset += written;
    }
    return new Text(bytes);
  }

  @Override
  public ColumnVector nextVector(ColumnVector previous, long batchSize) throws IOException {
    BytesColumnVector result = null;
    if (previous == null) {
      result = new BytesColumnVector();
    } else {
      result = (BytesColumnVector) previous;
    }

    RecordReaderImpl.BytesColumnVectorUtil
        .readOrcByteArrays(data, lengths, scratchlcv, result, batchSize);
    return result;
  }
}
