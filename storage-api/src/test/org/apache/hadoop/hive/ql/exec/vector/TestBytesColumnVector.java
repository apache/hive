/*
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

package org.apache.hadoop.hive.ql.exec.vector;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestBytesColumnVector {
  @Test
  public void testSmallBufferReuse() {
    BytesColumnVector col = new BytesColumnVector();
    int smallWriteSize = 1024;
    int largeWriteSize = 1024 * 1024 * 2;

    int rowIdx = 0;
    int bytesWrittenToBytes1 = 0;
    col.reset();

    // Initial write (small value)
    byte[] bytes1 = writeToBytesColumnVector(rowIdx, col, smallWriteSize, 1);
    bytesWrittenToBytes1 += smallWriteSize;
    assertEquals(0, col.start[0]);
    assertEquals(smallWriteSize, col.length[0]);

    // Write a large value. This should use a different byte buffer
    rowIdx++;
    byte[] bytes2 = writeToBytesColumnVector(rowIdx, col, largeWriteSize, -1);
    assertNotSame(bytes1, bytes2);
    assertEquals(0, col.start[1]);
    assertEquals(largeWriteSize, col.length[1]);

    // Another small write. smallBuffer should be re-used for this write
    rowIdx++;
    byte[] bytes3 = writeToBytesColumnVector(rowIdx, col, smallWriteSize, 2);
    bytesWrittenToBytes1 += smallWriteSize;
    assertSame(bytes1, bytes3);
    assertEquals(smallWriteSize, col.start[2]);
    assertEquals(smallWriteSize, col.length[2]);

    // Write another large value. This should use a different byte buffer
    rowIdx++;
    byte[] bytes4 = writeToBytesColumnVector(rowIdx, col, largeWriteSize, -2);
    assertNotSame(bytes1, bytes4);
    assertNotSame(bytes2, bytes4);
    assertEquals(0, col.start[3]);
    assertEquals(largeWriteSize, col.length[3]);

    // Eventually enough small writes should result in another buffer getting created
    boolean gotNewBuffer = false;
    // Test is dependent on getting a new buffer within 1MB.
    // This may need to change as the implementation changes.
    for (int i = 3; i < 1024; ++i) {
      rowIdx++;
      byte[] currBytes = writeToBytesColumnVector(rowIdx, col, smallWriteSize, i);
      if (currBytes == bytes1) {
        bytesWrittenToBytes1 += smallWriteSize;
      } else {
        gotNewBuffer = true;
        break;
      }
    }

    assertTrue(gotNewBuffer);

    // All small writes to the first buffer should be in contiguous memory
    for (int i = 0; i < bytesWrittenToBytes1; ++i) {
      assertEquals((byte) (i / smallWriteSize + 1), bytes1[i]);
    }
  }

  /**
   * Test the setVal, setConcat, and StringExpr.padRight methods.
   */
  @Test
  public void testConcatAndPadding() {
    BytesColumnVector col = new BytesColumnVector();
    col.reset();
    byte[] prefix = "緑".getBytes(StandardCharsets.UTF_8);

    // fill the column with 'test'
    for(int row=0; row < col.vector.length; ++row) {
      col.setVal(row, prefix, 0, prefix.length);
    }
    for(int row=0; row < col.vector.length; ++row) {
      assertEquals("row " + row, "緑", col.toString(row));
    }

    // pad out to 6 characters
    for(int row=0; row < col.vector.length; ++row) {
      StringExpr.padRight(col, row, col.vector[row], col.start[row],
          col.length[row], 6);
    }
    for(int row=0; row < col.vector.length; ++row) {
      assertEquals("row " + row, "緑     ", col.toString(row));
    }

    // concat the row digits
    for(int row=0; row < col.vector.length; ++row) {
      byte[] rowStr = Integer.toString(row).getBytes(StandardCharsets.UTF_8);
      col.setConcat(row, col.vector[row], col.start[row], col.length[row],
          rowStr, 0, rowStr.length);
    }
    for(int row=0; row < col.vector.length; ++row) {
      assertEquals("row " + row, "緑     " + row, col.toString(row));
    }

    // We end up allocating 20k, so we should have expanded the small buffer
    assertEquals(32 * 1024, col.bufferSize());
  }

  @Test
  public void testBufferOverflow() {
    BytesColumnVector col = new BytesColumnVector(2048);
    col.reset();
    assertEquals(BytesColumnVector.DEFAULT_BUFFER_SIZE, col.bufferSize());

    // pick a size below 1m so that we use the small buffer;
    final int size = BytesColumnVector.MAX_SIZE_FOR_SMALL_ITEM - 1024;

    // run through once to expand the small value buffer
    for(int row=0; row < col.vector.length; ++row) {
      writeToBytesColumnVector(row, col, size, row);
    }
    // it should have resized a bunch of times
    byte[] sharedBuffer = col.getValPreallocatedBytes();
    assertNotSame(sharedBuffer, col.vector[0]);
    assertSame(sharedBuffer, col.vector[1024]);

    // reset the column, but make sure the buffer isn't reallocated
    col.reset();
    assertEquals(BytesColumnVector.MAX_SIZE_FOR_SHARED_BUFFER, col.bufferSize());

    // fill up the vector now with the large buffer
    for(int row=0; row < col.vector.length; ++row) {
      writeToBytesColumnVector(row, col, size, row);
    }
    assertEquals(BytesColumnVector.MAX_SIZE_FOR_SHARED_BUFFER, col.bufferSize());
    // Now the first 1025 rows should all be the shared buffer,
    // because 1025 * size < MAX_SIZE_FOR_SMALL_BUFFER
    for(int row=0; row < 1025; ++row) {
      assertSame("row " + row, sharedBuffer, col.vector[row]);
      assertEquals("row " + row, row * size, col.start[row]);
      assertEquals("row " + row, size, col.length[row]);
    }
    // the rest should be custom buffers
    for(int row=1025; row < col.vector.length; ++row) {
      assertNotSame("row " + row, sharedBuffer, col.vector[row]);
      assertEquals("row " + row, 0, col.start[row]);
      assertEquals("row " + row, size, col.length[row]);
    }
  }

  /**
   * Make sure that ORC's redact mask won't get into trouble by increasing
   * the size of the allocation before calling setValPreallocated.
   */
  @Test
  public void testMultipleReallocations() {
    BytesColumnVector vector = new BytesColumnVector();
    vector.reset();

    // add a value
    vector.ensureValPreallocated(1000);
    byte[] bytes = vector.getValPreallocatedBytes();
    assertEquals(16 * 1024, bytes.length);
    assertEquals(0, vector.getValPreallocatedStart());
    // now ask for more memory
    vector.ensureValPreallocated(5000);
    // It shouldn't have needed to resize
    assertSame(bytes, vector.getValPreallocatedBytes());
    assertEquals(0, vector.getValPreallocatedStart());
    // actually use 2000 of the bytes
    vector.setValPreallocated(0, 2000);

    // add another value
    vector.ensureValPreallocated(10000);
    assertSame(bytes, vector.getValPreallocatedBytes());
    assertEquals(2000, vector.getValPreallocatedStart());
    vector.setValPreallocated(1, 3000);

    // check to make sure the values are allocated correctly
    assertSame(bytes, vector.vector[0]);
    assertEquals(0, vector.start[0]);
    assertEquals(2000, vector.length[0]);
    assertSame(bytes, vector.vector[1]);
    assertEquals(2000, vector.start[1]);
    assertEquals(3000, vector.length[1]);
  }

  // Write a value to the column vector, and return back the byte buffer used.
  private static byte[] writeToBytesColumnVector(int rowIdx, BytesColumnVector col, int writeSize, int val) {
    col.ensureValPreallocated(writeSize);
    byte[] bytes = col.getValPreallocatedBytes();
    int startIdx = col.getValPreallocatedStart();
    Arrays.fill(bytes, startIdx, startIdx + writeSize, (byte) val);
    col.setValPreallocated(rowIdx, writeSize);
    return bytes;
  }
}
