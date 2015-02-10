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

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.orc.DynamicByteArray;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.IntegerReader;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class DictionaryStringReader implements StringReader {
  private DynamicByteArray dictionaryBuffer;
  private IntegerReader data;
  private IntegerReader lengths;
  private InStream dictStream;
  private int[] dictionaryOffsets;
  private byte[] dictionaryBufferInBytesCache = null;
  private final LongColumnVector scratchlcv;
  private int dictionarySize;

  public DictionaryStringReader(IntegerReader lengths, IntegerReader data, InStream dictionary,
      int dictionarySize)
      throws IOException {
    this.lengths = lengths;
    this.dictionaryBuffer = null;
    this.dictStream = dictionary;
    this.dictionaryOffsets = null;
    this.dictionarySize = dictionarySize;
    this.data = data;
    this.scratchlcv = new LongColumnVector();
    readDictionary();
  }

  private void readDictionary() throws IOException {
    if (dictionaryBuffer == null) {
      dictionaryBuffer = new DynamicByteArray(64, dictStream.available());
      this.dictionaryBuffer.readAll(dictStream);
    }

    int offset = 0;
    if (dictionaryOffsets == null ||
        dictionaryOffsets.length < dictionarySize + 1) {
      dictionaryOffsets = new int[dictionarySize + 1];
    }
    for (int i = 0; i < dictionarySize; ++i) {
      dictionaryOffsets[i] = offset;
      offset += (int) lengths.next();
    }
    dictionaryOffsets[dictionarySize] = offset;
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void skip(long numValues) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean hasNext() throws IOException {
    return data.hasNext();
  }

  @Override
  public Text next() throws IOException {
    int entry = (int) data.next();
    int offset = dictionaryOffsets[entry];
    int length = getDictionaryEntryLength(entry, offset);
    Text result = new Text();
    dictionaryBuffer.setText(result, offset, length);
    return result;
  }

  int getDictionaryEntryLength(int entry, int offset) {
    int length = 0;
    // if it isn't the last entry, subtract the offsets otherwise use
    // the buffer length.
    if (entry < dictionaryOffsets.length - 1) {
      length = dictionaryOffsets[entry + 1] - offset;
    } else {
      length = dictionaryBuffer.size() - offset;
    }
    return length;
  }

  @Override
  public ColumnVector nextVector(ColumnVector previous, long batchSize) throws IOException {
    BytesColumnVector result = null;
    int offset = 0, length = 0;
    if (previous == null) {
      result = new BytesColumnVector();
    } else {
      result = (BytesColumnVector) previous;
    }

    if (dictionaryBuffer != null) {

      // Load dictionaryBuffer into cache.
      if (dictionaryBufferInBytesCache == null) {
        dictionaryBufferInBytesCache = dictionaryBuffer.get();
      }

      // Read string offsets
      scratchlcv.isNull = result.isNull;
      lengths.nextVector(scratchlcv, batchSize);
      if (!scratchlcv.isRepeating) {

        // The vector has non-repeating strings. Iterate thru the batch
        // and set strings one by one
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            offset = dictionaryOffsets[(int) scratchlcv.vector[i]];
            length = getDictionaryEntryLength((int) scratchlcv.vector[i], offset);
            result.setRef(i, dictionaryBufferInBytesCache, offset, length);
          } else {
            // If the value is null then set offset and length to zero (null string)
            result.setRef(i, dictionaryBufferInBytesCache, 0, 0);
          }
        }
      } else {
        // If the value is repeating then just set the first value in the
        // vector and set the isRepeating flag to true. No need to iterate thru and
        // set all the elements to the same value
        offset = dictionaryOffsets[(int) scratchlcv.vector[0]];
        length = getDictionaryEntryLength((int) scratchlcv.vector[0], offset);
        result.setRef(0, dictionaryBufferInBytesCache, offset, length);
      }
      result.isRepeating = scratchlcv.isRepeating;
    } else {
      // Entire stripe contains null strings.
      result.isRepeating = true;
      result.noNulls = false;
      result.isNull[0] = true;
      result.setRef(0, "".getBytes(), 0, 0);
    }
    return result;
  }
}
