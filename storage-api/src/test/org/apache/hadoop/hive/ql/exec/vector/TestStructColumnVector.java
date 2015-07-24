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

package org.apache.hadoop.hive.ql.exec.vector;

import org.junit.Test;

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
    output.setElement(3, 6, input);
    StringBuilder buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("null", buf.toString());
    output.setElement(3, 5, input);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[1, null]", buf.toString());
    output.setElement(3, 4, input);
    buf = new StringBuilder();
    output.stringifyValue(buf, 3);
    assertEquals("[1, 6]", buf.toString());
    input.reset();
    assertEquals(false, input1.isRepeating);
    assertEquals(true, input.noNulls);
  }
}
