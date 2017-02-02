/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.thrift;

import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestColumnBuffer {
  @Rule
  public RepeatingRule repeatingRule = new RepeatingRule();

  private static final int NUM_VARS = 100;
  private static final int NUM_NULLS = 30;
  private static final Set<Integer> nullIndices = new HashSet<>();

  private final Type type;
  private final Object vars;

  @Parameterized.Parameters
  public static Collection<Object[]> types() {
    return Arrays.asList(new Object[][]{
            {Type.BOOLEAN_TYPE},
            {Type.TINYINT_TYPE},
            {Type.SMALLINT_TYPE},
            {Type.INT_TYPE},
            {Type.BIGINT_TYPE},
            {Type.DOUBLE_TYPE},
            {Type.FLOAT_TYPE},
            {Type.BINARY_TYPE},
            {Type.STRING_TYPE}
        }
    );
  }

  public TestColumnBuffer(Type type) {
    this.type = type;
    switch (type) {
      case BOOLEAN_TYPE:
        vars = new boolean[NUM_VARS];
        break;
      case TINYINT_TYPE:
        vars = new byte[NUM_VARS];
        break;
      case SMALLINT_TYPE:
        vars = new short[NUM_VARS];
        break;
      case INT_TYPE:
        vars = new int[NUM_VARS];
        break;
      case BIGINT_TYPE:
        vars = new long[NUM_VARS];
        break;
      case DOUBLE_TYPE:
      case FLOAT_TYPE:
        vars = new double[NUM_VARS];
        break;
      case BINARY_TYPE:
        vars = Arrays.asList(new ByteBuffer[NUM_VARS]);
        break;
      case STRING_TYPE:
        vars = Arrays.asList(new String[NUM_VARS]);
        break;
      default:
        throw new IllegalArgumentException("Invalid type " + type);
    }
  }

  private static void prepareNullIndices() {
    nullIndices.clear();
    Random random = ThreadLocalRandom.current();
    while (nullIndices.size() != NUM_NULLS) {
      nullIndices.add(random.nextInt(NUM_VARS));
    }
  }

  /**
   * Test if the nulls BitSet is maintained properly when we extract subset from ColumnBuffer.
   * E.g. suppose we have a ColumnBuffer with nulls [0, 0, 1, 0]. When we split it evenly into
   * two subsets, the subsets should have nulls [0, 0] and [1, 0] respectively.
   */
  @Test
  @Repeating(repetition = 10)
  public void testNullsInSubset() {
    prepareNullIndices();
    BitSet nulls = new BitSet(NUM_VARS);
    for (int index : nullIndices) {
      nulls.set(index);
    }

    ColumnBuffer columnBuffer = new ColumnBuffer(type, nulls, vars);
    Random random = ThreadLocalRandom.current();

    int remaining = NUM_VARS;
    while (remaining > 0) {
      int toExtract = random.nextInt(remaining) + 1;
      ColumnBuffer subset = columnBuffer.extractSubset(toExtract);
      verifyNulls(subset, NUM_VARS - remaining);
      remaining -= toExtract;
    }
  }

  private static void verifyNulls(ColumnBuffer buffer, int shift) {
    BitSet nulls = buffer.getNulls();
    for (int i = 0; i < buffer.size(); i++) {
      Assert.assertEquals("BitSet in parent and subset not the same.",
          nullIndices.contains(i + shift), nulls.get(i));
    }
  }

  @Test
  public void testAddValues() {
    switch (type) {
      case BOOLEAN_TYPE:
        testBooleanValues();
        break;
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
      case INT_TYPE:
      case BIGINT_TYPE:
        testAllIntegerTypeValues();
        break;
      case DOUBLE_TYPE:
      case FLOAT_TYPE:
        testFloatAndDoubleValues();
        break;
      case BINARY_TYPE:
        testBinaryValues();
        break;
      case STRING_TYPE:
        testStringValues();
        break;
      default:
        throw new IllegalArgumentException("Invalid type " + type);
    }
  }


  private void testAllIntegerTypeValues() {
    Map<Type, List<Object>> integerTypesAndValues = new LinkedHashMap<Type, List<Object>>();

    // Add TINYINT values
    integerTypesAndValues.put(Type.TINYINT_TYPE, Arrays.<Object>asList(
        Byte.MIN_VALUE, Byte.MAX_VALUE
    ));

    // Add SMALLINT values
    integerTypesAndValues.put(Type.SMALLINT_TYPE, Arrays.<Object>asList(
        Short.MIN_VALUE, Short.MIN_VALUE
    ));

    // Add INT values
    integerTypesAndValues.put(Type.INT_TYPE, Arrays.<Object>asList(
        Integer.MIN_VALUE, Integer.MAX_VALUE
    ));

    // Add BIGINT values
    integerTypesAndValues.put(Type.BIGINT_TYPE, Arrays.<Object>asList(
        Long.MIN_VALUE, Long.MAX_VALUE
    ));

    // Validate all integer type values are stored correctly
    for (Map.Entry entry : integerTypesAndValues.entrySet()) {
      Type type = (Type) entry.getKey();
      List<Object> values = (List) entry.getValue();

      ColumnBuffer c = new ColumnBuffer(type);
      for (Object v : values) {
        c.addValue(type, v);
      }

      assertEquals(type, c.getType());
      assertEquals(values.size(), c.size());

      for (int i = 0; i < c.size(); i++) {
        assertEquals(values.get(i), c.get(i));
      }
    }
  }


  private void testFloatAndDoubleValues() {
    ColumnBuffer floatColumn = new ColumnBuffer(Type.FLOAT_TYPE);
    floatColumn.addValue(Type.FLOAT_TYPE, 1.1f);
    floatColumn.addValue(Type.FLOAT_TYPE, 2.033f);

    // FLOAT_TYPE is treated as DOUBLE_TYPE
    assertEquals(Type.FLOAT_TYPE, floatColumn.getType());
    assertEquals(2, floatColumn.size());
    assertEquals(1.1, floatColumn.get(0));
    assertEquals(2.033, floatColumn.get(1));

    ColumnBuffer doubleColumn = new ColumnBuffer(Type.DOUBLE_TYPE);
    doubleColumn.addValue(Type.DOUBLE_TYPE, 1.1);
    doubleColumn.addValue(Type.DOUBLE_TYPE, 2.033);

    assertEquals(Type.DOUBLE_TYPE, doubleColumn.getType());
    assertEquals(2, doubleColumn.size());
    assertEquals(1.1, doubleColumn.get(0));
    assertEquals(2.033, doubleColumn.get(1));
  }


  private void testBooleanValues() {
    ColumnBuffer boolColumn = new ColumnBuffer(Type.BOOLEAN_TYPE);
    boolColumn.addValue(Type.BOOLEAN_TYPE, true);
    boolColumn.addValue(Type.BOOLEAN_TYPE, false);

    assertEquals(Type.BOOLEAN_TYPE, boolColumn.getType());
    assertEquals(2, boolColumn.size());
    assertEquals(true, boolColumn.get(0));
    assertEquals(false, boolColumn.get(1));
  }


  private void testStringValues() {
    ColumnBuffer stringColumn = new ColumnBuffer(Type.STRING_TYPE);
    stringColumn.addValue(Type.STRING_TYPE, "12abc456");
    stringColumn.addValue(Type.STRING_TYPE, "~special$&string");

    assertEquals(Type.STRING_TYPE, stringColumn.getType());
    assertEquals(2, stringColumn.size());
    assertEquals("12abc456", stringColumn.get(0));
    assertEquals("~special$&string", stringColumn.get(1));
  }


  private void testBinaryValues() {
    ColumnBuffer binaryColumn = new ColumnBuffer(Type.BINARY_TYPE);
    binaryColumn.addValue(Type.BINARY_TYPE, new byte[]{-1, 0, 3, 4});

    assertEquals(Type.BINARY_TYPE, binaryColumn.getType());
    assertEquals(1, binaryColumn.size());
    assertArrayEquals(new byte[]{-1, 0, 3, 4}, (byte[]) binaryColumn.get(0));
  }
}
