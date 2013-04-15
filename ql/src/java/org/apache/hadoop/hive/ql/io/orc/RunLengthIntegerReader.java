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

/**
 * A reader that reads a sequence of integers.
 * */
class RunLengthIntegerReader {
  private final InStream input;
  private final boolean signed;
  private final long[] literals =
    new long[RunLengthIntegerWriter.MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private int delta = 0;
  private int used = 0;
  private boolean repeat = false;

  RunLengthIntegerReader(InStream input, boolean signed) throws IOException {
    this.input = input;
    this.signed = signed;
  }

  private void readValues() throws IOException {
    int control = input.read();
    if (control == -1) {
      throw new EOFException("Read past end of RLE integer from " + input);
    } else if (control < 0x80) {
      numLiterals = control + RunLengthIntegerWriter.MIN_REPEAT_SIZE;
      used = 0;
      repeat = true;
      delta = input.read();
      if (delta == -1) {
        throw new EOFException("End of stream in RLE Integer from " + input);
      }
      // convert from 0 to 255 to -128 to 127 by converting to a signed byte
      delta = (byte) (0 + delta);
      if (signed) {
        literals[0] = SerializationUtils.readVslong(input);
      } else {
        literals[0] = SerializationUtils.readVulong(input);
      }
    } else {
      repeat = false;
      numLiterals = 0x100 - control;
      used = 0;
      for(int i=0; i < numLiterals; ++i) {
        if (signed) {
          literals[i] = SerializationUtils.readVslong(input);
        } else {
          literals[i] = SerializationUtils.readVulong(input);
        }
      }
    }
  }

  boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

  long next() throws IOException {
    long result;
    if (used == numLiterals) {
      readValues();
    }
    if (repeat) {
      result = literals[0] + (used++) * delta;
    } else {
      result = literals[used++];
    }
    return result;
  }

  void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two parts
      while (consumed > 0) {
        readValues();
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

  void skip(long numValues) throws IOException {
    while (numValues > 0) {
      if (used == numLiterals) {
        readValues();
      }
      long consume = Math.min(numValues, numLiterals - used);
      used += consume;
      numValues -= consume;
    }
  }
}
