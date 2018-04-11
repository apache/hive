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

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for StructColumnVector
 */
public class TestStructColumnVector {

  @Test
  public void testFlatten() throws Exception {
    LongColumnVector col1 = new LongColumnVector(10);
    LongColumnVector col2 = new LongColumnVector(10);
    StructColumnVector vector = new StructColumnVector(10, col1, col2);
    vector.init();
    col1.isRepeating = true;
    for(int i=0; i < 10; ++i) {
      col1.vector[i] = i;
      col2.vector[i] = 2 * i;
    }
    vector.flatten(false, null, 10);
    assertFalse(col1.isRepeating);
    for(int i=0; i < 10; ++i) {
      assertEquals("col1 at " + i, 0, col1.vector[i]);
      assertEquals("col2 at " + i, 2 * i, col2.vector[i]);
    }
    vector.unFlatten();
    assertTrue(col1.isRepeating);
    for(int i=0; i < 10; ++i) {
      StringBuilder buf = new StringBuilder();
      vector.stringifyValue(buf, i);
      assertEquals("[0, " + (2 * i) + "]", buf.toString());
    }
    vector.reset();
    assertFalse(col1.isRepeating);
  }

  @Test
  public void testSet() throws Exception {
    LongColumnVector input1 = new LongColumnVector(10);
    LongColumnVector input2 = new LongColumnVector(10);
    StructColumnVector input = new StructColumnVector(10, input1, input2);
    input.init();
    LongColumnVector output1 = new LongColumnVector(10);
    LongColumnVector output2 = new LongColumnVector(10);
    StructColumnVector output = new StructColumnVector(10, output1, output2);
    output.init();
    input1.isRepeating = true;
    input2.noNulls = false;
    input2.isNull[5] = true;
    input.noNulls = false;
    input.isNull[6] = true;
    for(int i=0; i < 10; ++i) {
      input1.vector[i] = i + 1;
      input2.vector[i] = i + 2;
    }
    output.isNull[3] = false;
    output.setElement(3, 6, input);
    StringBuilder buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("null", buf.toString());
    output.isNull[3] = false;
    output.setElement(3, 5, input);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[1, null]", buf.toString());
    output.isNull[3] = false;
    output.setElement(3, 4, input);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[1, 6]", buf.toString());
    input.reset();
    assertEquals(false, input1.isRepeating);
    assertEquals(true, input.noNulls);
  }

  @Test
  public void testStringify() throws IOException {
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    LongColumnVector x1 = new LongColumnVector();
    TimestampColumnVector x2 = new TimestampColumnVector();
    StructColumnVector x = new StructColumnVector(1024, x1, x2);
    BytesColumnVector y = new BytesColumnVector();
    batch.cols[0] = x;
    batch.cols[1] = y;
    batch.reset();
    Timestamp ts = Timestamp.valueOf("2000-01-01 00:00:00");
    for(int r=0; r < 10; ++r) {
      batch.size += 1;
      x1.vector[r] = 3 * r;
      ts.setTime(ts.getTime() + 1000);
      x2.set(r, ts);
      byte[] buffer = ("value " + r).getBytes(StandardCharsets.UTF_8);
      y.setRef(r, buffer, 0, buffer.length);
    }
    final String EXPECTED = ("Column vector types: 0:STRUCT, 1:BYTES\n" +
        "[[0, 2000-01-01 00:00:01.0], \"value 0\"]\n" +
        "[[3, 2000-01-01 00:00:02.0], \"value 1\"]\n" +
        "[[6, 2000-01-01 00:00:03.0], \"value 2\"]\n" +
        "[[9, 2000-01-01 00:00:04.0], \"value 3\"]\n" +
        "[[12, 2000-01-01 00:00:05.0], \"value 4\"]\n" +
        "[[15, 2000-01-01 00:00:06.0], \"value 5\"]\n" +
        "[[18, 2000-01-01 00:00:07.0], \"value 6\"]\n" +
        "[[21, 2000-01-01 00:00:08.0], \"value 7\"]\n" +
        "[[24, 2000-01-01 00:00:09.0], \"value 8\"]\n" +
        "[[27, 2000-01-01 00:00:10.0], \"value 9\"]");
    assertEquals(EXPECTED, batch.toString());
  }
}
