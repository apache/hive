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

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.BitFieldReader;

public class LlapUtils {
  public static final int DOUBLE_GROUP_SIZE = 64; // just happens to be equal to bitmask size

  /** Helper for readPresentStream. */
  public static class PresentStreamReadResult {
    public int availLength;
    public boolean isNullsRun = false;
    public boolean isFollowedByOther = false;
    public void reset() {
      isFollowedByOther = isNullsRun = false;
    }
  }

  /**
   * Helper method that reads present stream to find the size of the run of nulls, or a
   * count of contiguous of non-null values based on the run length from the main stream.
   * @param r Result is returned via this because java is not a real language.
   * @param present The present stream.
   * @param availLength The run length from the main stream.
   * @param rowsLeftToRead Total number of rows that may be read from the main stream.
   */
  public static void readPresentStream(PresentStreamReadResult r,
      BitFieldReader present, long rowsLeftToRead) throws IOException {
    int presentBitsRead = 0;
    r.reset();
    // We are looking for a run of nulls no longer than rows to rowsLeftToRead (presumes
    // they will all fit in the writer), OR a run of non-nulls no longer than availLength.

    // If there's a partial byte in present stream, we will read bits from it.
    boolean doneWithPresent = false;
    int rowLimit = r.availLength;
    while (!present.hasFullByte() && !doneWithPresent
        && (presentBitsRead == 0 || (presentBitsRead < rowLimit))) {
      int bit = present.peekOneBit();
      if (presentBitsRead == 0) {
        r.isNullsRun = (bit == 0);
        rowLimit = (int)(r.isNullsRun ? rowsLeftToRead : r.availLength);
      } else if (r.isNullsRun != (bit == 0)) {
        doneWithPresent = r.isFollowedByOther = true;
        break;
      }
      present.skipInCurrentByte(1);
      ++presentBitsRead;
    }
    // Now, if we are not done, read the full bytes.
    // TODO: we could ask the underlying byte stream of "present" reader for runs;
    //       many bitmasks might have long sequences of 0x00 or 0xff.
    while (!doneWithPresent && (presentBitsRead == 0 || (presentBitsRead < rowLimit))) {
      int bits = present.peekFullByte();
      if (presentBitsRead == 0) {
        r.isNullsRun = (bits & (1 << 7)) == 0;
        rowLimit = (int)(r.isNullsRun ? rowsLeftToRead : r.availLength);
      }
      int bitsToTake = 0;
      if ((bits == 0 && r.isNullsRun) || (bits == 0xff && !r.isNullsRun)) {
        bitsToTake = 8;
      } else {
        doneWithPresent = r.isFollowedByOther = true;
        // Get the number of leading 0s or 1s in this byte.
        bitsToTake = Integer.numberOfLeadingZeros(r.isNullsRun ? bits : (~(bits | ~255))) - 24;
      }
      bitsToTake = Math.min(bitsToTake, rowLimit - presentBitsRead);
      presentBitsRead += bitsToTake;
      present.skipInCurrentByte(bitsToTake);
    } // End of the loop reading full bytes.
    assert presentBitsRead <= rowLimit :  "Read " + presentBitsRead + " bits for "
    + (r.isNullsRun ? "" : "non-") + "null run: " + rowsLeftToRead + ", " + r.availLength;
    assert presentBitsRead > 0;
    r.availLength = presentBitsRead;
  }
}
