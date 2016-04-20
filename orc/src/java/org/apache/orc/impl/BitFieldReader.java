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
package org.apache.orc.impl;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RunLengthByteReader;

public class BitFieldReader {
  private final RunLengthByteReader input;
  /** The number of bits in one item. Non-test code always uses 1. */
  private final int bitSize;
  private int current;
  private int bitsLeft;
  private final int mask;

  public BitFieldReader(InStream input,
      int bitSize) throws IOException {
    this.input = new RunLengthByteReader(input);
    this.bitSize = bitSize;
    mask = (1 << bitSize) - 1;
  }

  public void setInStream(InStream inStream) {
    this.input.setInStream(inStream);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  public int next() throws IOException {
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

  /**
   * Unlike integer readers, where runs are encoded explicitly, in this one we have to read ahead
   * to figure out whether we have a run. Given that runs in booleans are likely it's worth it.
   * However it means we'd need to keep track of how many bytes we read, and next/nextVector won't
   * work anymore once this is called. These is trivial to fix, but these are never interspersed.
   */
  private boolean lastRunValue;
  private int lastRunLength = -1;
  private void readNextRun(int maxRunLength) throws IOException {
    assert bitSize == 1;
    if (lastRunLength > 0) return; // last run is not exhausted yet
    if (bitsLeft == 0) {
      readByte();
    }
    // First take care of the partial bits.
    boolean hasVal = false;
    int runLength = 0;
    if (bitsLeft != 8) {
      int partialBitsMask = (1 << bitsLeft) - 1;
      int partialBits = current & partialBitsMask;
      if (partialBits == partialBitsMask || partialBits == 0) {
        lastRunValue = (partialBits == partialBitsMask);
        if (maxRunLength <= bitsLeft) {
          lastRunLength = maxRunLength;
          return;
        }
        maxRunLength -= bitsLeft;
        hasVal = true;
        runLength = bitsLeft;
        bitsLeft = 0;
      } else {
        // There's no run in partial bits. Return whatever we have.
        int prefixBitsCount = 32 - bitsLeft;
        runLength = Integer.numberOfLeadingZeros(partialBits) - prefixBitsCount;
        lastRunValue = (runLength > 0);
        lastRunLength = Math.min(maxRunLength, lastRunValue ? runLength :
          (Integer.numberOfLeadingZeros(~(partialBits | ~partialBitsMask)) - prefixBitsCount));
        return;
      }
      assert bitsLeft == 0;
      readByte();
    }
    if (!hasVal) {
      lastRunValue = ((current >> 7) == 1);
      hasVal = true;
    }
    // Read full bytes until the run ends.
    assert bitsLeft == 8;
    while (maxRunLength >= 8
        && ((lastRunValue && (current == 0xff)) || (!lastRunValue && (current == 0)))) {
      runLength += 8;
      maxRunLength -= 8;
      readByte();
    }
    if (maxRunLength > 0) {
      int extraBits = Integer.numberOfLeadingZeros(
          lastRunValue ? (~(current | ~255)) : current) - 24;
      bitsLeft -= extraBits;
      runLength += extraBits;
    }
    lastRunLength = runLength;
  }

  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (!previous.isNull[i]) {
        previous.vector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((previous.vector[i - 1] != previous.vector[i]) || (previous.isNull[i - 1] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
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

  public void skip(long items) throws IOException {
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

  @Override
  public String toString() {
    return "bit reader current: " + current + " bits left: " + bitsLeft +
        " bit size: " + bitSize + " from " + input;
  }

  boolean hasFullByte() {
    return bitsLeft == 8 || bitsLeft == 0;
  }

  int peekOneBit() throws IOException {
    assert bitSize == 1;
    if (bitsLeft == 0) {
      readByte();
    }
    return (current >>> (bitsLeft - 1)) & 1;
  }

  int peekFullByte() throws IOException {
    assert bitSize == 1;
    assert bitsLeft == 8 || bitsLeft == 0;
    if (bitsLeft == 0) {
      readByte();
    }
    return current;
  }

  void skipInCurrentByte(int bits) throws IOException {
    assert bitsLeft >= bits;
    bitsLeft -= bits;
  }
}
