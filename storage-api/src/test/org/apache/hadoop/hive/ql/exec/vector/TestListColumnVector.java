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

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for ListColumnVector
 */
public class TestListColumnVector {

  @Test
  public void testFlatten() throws Exception {
    LongColumnVector col1 = new LongColumnVector(10);
    ListColumnVector vector = new ListColumnVector(10, col1);
    vector.init();

    // TEST - repeating NULL & no selection
    col1.isRepeating = true;
    vector.isRepeating = true;
    vector.noNulls = false;
    vector.isNull[0] = true;
    vector.childCount = 0;
    for(int i=0; i < 10; ++i) {
      col1.vector[i] = i + 3;
      vector.offsets[i] = i;
      vector.lengths[i] = 10 + i;
    }
    vector.flatten(false, null, 10);
    // make sure the vector was flattened
    assertFalse(vector.isRepeating);
    assertFalse(vector.noNulls);
    // child isn't flattened, because parent is repeating null
    assertTrue(col1.isRepeating);
    assertTrue(col1.noNulls);
    for(int i=0; i < 10; ++i) {
      assertTrue("isNull at " + i, vector.isNull[i]);
    }
    for(int i=0; i < 10; ++i) {
      StringBuilder buf = new StringBuilder();
      vector.stringifyValue(buf, i);
      assertEquals("null", buf.toString());
    }
    vector.unFlatten();
    assertTrue(col1.isRepeating);
    assertTrue(vector.isRepeating);

    // TEST - repeating NULL & selection
    Arrays.fill(vector.isNull, 1, 10, false);
    int[] sel = new int[]{3, 5, 7};
    vector.flatten(true, sel, 3);
    for(int i=1; i < 10; i++) {
      assertEquals("failure at " + i,
          i == 3 || i == 5 || i == 7, vector.isNull[i]);
    }
    vector.unFlatten();

    // TEST - repeating non-NULL & no-selection
    vector.noNulls = true;
    vector.isRepeating = true;
    vector.offsets[0] = 0;
    vector.lengths[0] = 3;
    vector.childCount = 3;
    vector.flatten(false, null, 10);
    // make sure the vector was flattened
    assertFalse(vector.isRepeating);
    assertFalse(vector.noNulls);
    assertFalse(col1.isRepeating);
    assertFalse(col1.noNulls);
    for(int i=0; i < 10; ++i) {
      assertEquals("offset at " + i, 0, vector.offsets[i]);
      assertEquals("length at " + i, 3, vector.lengths[i]);
    }
    for(int i=0; i < 10; ++i) {
      StringBuilder buf = new StringBuilder();
      vector.stringifyValue(buf, i);
      assertEquals("[3, 3, 3]", buf.toString());
    }
    vector.unFlatten();
    assertTrue(col1.isRepeating);
    assertTrue(col1.noNulls);
    assertTrue(vector.isRepeating);
    assertTrue(vector.noNulls);

    // TEST - repeating non-NULL & selection
    Arrays.fill(vector.offsets, 1, 10, -1);
    Arrays.fill(vector.lengths, 1, 10, -1);
    Arrays.fill(col1.vector, 1, 10, -1);
    vector.flatten(true, sel, 3);
    for(int i=1; i < 10; i++) {
      if (i == 3 || i == 5 || i == 7) {
        assertEquals("failure at " + i, 0, vector.offsets[i]);
        assertEquals("failure at " + i, 3, vector.lengths[i]);
      } else {
        assertEquals("failure at " + i, -1, vector.offsets[i]);
        assertEquals("failure at " + i, -1, vector.lengths[i]);
      }
    }
    for(int i=0; i < 3; ++i) {
      assertEquals("failure at " + i, 3, col1.vector[i]);
    }
    for(int i=3; i < 10; ++i) {
      assertEquals("failure at " + i, -1, col1.vector[i]);
    }
    vector.unFlatten();

    // TEST - reset
    vector.reset();
    assertFalse(col1.isRepeating);
    assertTrue(col1.noNulls);
    assertFalse(vector.isRepeating);
    assertTrue(vector.noNulls);
    assertEquals(0, vector.childCount);
  }

  @Test
  public void testSet() throws Exception {
    LongColumnVector input1 = new LongColumnVector(10);
    ListColumnVector input = new ListColumnVector(10, input1);
    input.init();
    LongColumnVector output1 = new LongColumnVector(30);
    ListColumnVector output = new ListColumnVector(10, output1);
    output.init();
    input.noNulls = false;
    input.isNull[6] = true;
    input.childCount = 11;
    Arrays.fill(output1.vector, -1);
    for(int i=0; i < 10; ++i) {
      input1.vector[i] = 10 * i;
      input.offsets[i] = i;
      input.lengths[i] = 2;
      output.offsets[i] = i + 2;
      output.lengths[i] = 3;
    }
    output.childCount = 30;

    // copy a null
    output.isNull[3] = false;
    output.setElement(3, 6, input);
    assertEquals(30, output.childCount);
    StringBuilder buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("null", buf.toString());

    // copy a value
    output.isNull[3] = false;
    output.setElement(3, 5, input);
    assertEquals(30, output.offsets[3]);
    assertEquals(2, output.lengths[3]);
    assertEquals(32, output.childCount);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[50, 60]", buf.toString());

    // overwrite a value
    output.isNull[3] = false;
    output.setElement(3, 4, input);
    assertEquals(34, output.childCount);
    assertEquals(34, output1.vector.length);
    assertEquals(50, output1.vector[30]);
    assertEquals(60, output1.vector[31]);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[40, 50]", buf.toString());

    input.reset();
    assertEquals(false, input1.isRepeating);
    assertEquals(true, input.noNulls);
    output.reset();
    assertEquals(0, output.childCount);

    input.isRepeating = true;
    input.offsets[0] = 0;
    input.lengths[0] = 10;
    output.setElement(2, 7, input);
    assertEquals(10, output.childCount);
    buf = new StringBuilder();
    output.stringifyValue(buf, 2);
    assertEquals("[0, 10, 20, 30, 40, 50, 60, 70, 80, 90]", buf.toString());
  }
}
