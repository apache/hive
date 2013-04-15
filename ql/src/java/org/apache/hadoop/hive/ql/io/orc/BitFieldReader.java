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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.EOFException;
import java.io.IOException;

class BitFieldReader {
  private RunLengthByteReader input;
  private final int bitSize;
  private int current;
  private int bitsLeft;
  private final int mask;

  BitFieldReader(InStream input,
                 int bitSize) throws IOException {
    this.input = new RunLengthByteReader(input);
    this.bitSize = bitSize;
    mask = (1 << bitSize) - 1;
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + input);
    }
  }

  int next() throws IOException {
    int result = 0;
    int bitsLeftToRead = bitSize;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= current & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;
      readByte();
    }
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= bitsLeftToRead;
      result |= (current >>> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    return result & mask;
  }

  void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      bitsLeft = 8 - consumed;
    } else {
      bitsLeft = 0;
    }
  }

  void skip(long items) throws IOException {
    long totalBits = bitSize * items;
    if (bitsLeft >= totalBits) {
      bitsLeft -= totalBits;
    } else {
      totalBits -= bitsLeft;
      input.skip(totalBits / 8);
      current = input.next();
      bitsLeft = (int) (8 - (totalBits % 8));
    }
  }
}
