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
package org.apache.hive.service.cli;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestColumn {
  @Test
  public void testAllIntegerTypeValues() {
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
      Type type = (Type)entry.getKey();
      List<Object> values = (List)entry.getValue();

      Column c = new Column(type);
      for (Object v : values) {
        c.addValue(type, v);
      }

      assertEquals(type, c.getType());
      assertEquals(values.size(), c.size());

      for (int i=0; i<c.size(); i++) {
        assertEquals(values.get(i), c.get(i));
      }
    }
  }

  @Test
  public void testFloatAndDoubleValues() {
    Column floatColumn = new Column(Type.FLOAT_TYPE);
    floatColumn.addValue(Type.FLOAT_TYPE, 1.1f);
    floatColumn.addValue(Type.FLOAT_TYPE, 2.033f);

    // FLOAT_TYPE is treated as DOUBLE_TYPE
    assertEquals(Type.DOUBLE_TYPE, floatColumn.getType());
    assertEquals(2, floatColumn.size());
    assertEquals(1.1, floatColumn.get(0));
    assertEquals(2.033, floatColumn.get(1));

    Column doubleColumn = new Column(Type.DOUBLE_TYPE);
    doubleColumn.addValue(Type.DOUBLE_TYPE, 1.1);
    doubleColumn.addValue(Type.DOUBLE_TYPE, 2.033);

    assertEquals(Type.DOUBLE_TYPE, doubleColumn.getType());
    assertEquals(2, doubleColumn.size());
    assertEquals(1.1, doubleColumn.get(0));
    assertEquals(2.033, doubleColumn.get(1));
  }

  @Test
  public void testBooleanValues() {
    Column boolColumn = new Column(Type.BOOLEAN_TYPE);
    boolColumn.addValue(Type.BOOLEAN_TYPE, true);
    boolColumn.addValue(Type.BOOLEAN_TYPE, false);

    assertEquals(Type.BOOLEAN_TYPE, boolColumn.getType());
    assertEquals(2, boolColumn.size());
    assertEquals(true, boolColumn.get(0));
    assertEquals(false, boolColumn.get(1));
  }

  @Test
  public void testStringValues() {
    Column stringColumn = new Column(Type.STRING_TYPE);
    stringColumn.addValue(Type.STRING_TYPE, "12abc456");
    stringColumn.addValue(Type.STRING_TYPE, "~special$&string");

    assertEquals(Type.STRING_TYPE, stringColumn.getType());
    assertEquals(2, stringColumn.size());
    assertEquals("12abc456", stringColumn.get(0));
    assertEquals("~special$&string", stringColumn.get(1));
  }

  @Test
  public void testBinaryValues() {
    Column binaryColumn = new Column(Type.BINARY_TYPE);
    binaryColumn.addValue(Type.BINARY_TYPE, new byte[]{-1, 0, 3, 4});

    assertEquals(Type.BINARY_TYPE, binaryColumn.getType());
    assertEquals(1, binaryColumn.size());
    assertArrayEquals(new byte[]{-1, 0, 3, 4}, (byte[]) binaryColumn.get(0));
  }
}
