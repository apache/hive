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
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory.DefaultExprProcessor;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Parametrized test for the TypeCheckProcFactory.
 *
 */
@RunWith(Parameterized.class)
public class TestTypeCheckProcFactory {
  @Mock
  private PrimitiveTypeInfo typeInfo;
  @Mock
  private ExprNodeConstantDesc nodeDesc;

  private DefaultExprProcessor testSubject;

  @Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{"127", PrimitiveObjectInspectorUtils.byteTypeEntry, (byte) 127, true},
        {"32767", PrimitiveObjectInspectorUtils.shortTypeEntry, (short) 32767, true},
        {"2147483647", PrimitiveObjectInspectorUtils.intTypeEntry, 2147483647, true},
        {"9223372036854775807", PrimitiveObjectInspectorUtils.longTypeEntry, 9223372036854775807L, true},
        {"111.1", PrimitiveObjectInspectorUtils.floatTypeEntry, 111.1f, false},
        {"111.1", PrimitiveObjectInspectorUtils.doubleTypeEntry, 111.1d, false}});
  }

  private final BigDecimal maxValue;
  private final PrimitiveTypeEntry constType;
  private final Object expectedValue;
  private final boolean intType;

  public TestTypeCheckProcFactory(String maxValue, PrimitiveTypeEntry constType, Object expectedValue,
      boolean intType) {
    this.maxValue = new BigDecimal(maxValue);
    this.constType = constType;
    this.expectedValue = expectedValue;
    this.intType = intType;
  }

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    testSubject = new DefaultExprProcessor();
  }

  public void testOneCase(Object constValue) {
    when(nodeDesc.getValue()).thenReturn(constValue);
    when(typeInfo.getPrimitiveTypeEntry()).thenReturn(constType);

    ExprNodeConstantDesc result = (ExprNodeConstantDesc) testSubject.interpretNodeAs(typeInfo, nodeDesc);

    assertNotNull(result);
    assertEquals(expectedValue, result.getValue());
  }

  public void testNullCase(Object constValue) {
    when(nodeDesc.getValue()).thenReturn(constValue);
    when(typeInfo.getPrimitiveTypeEntry()).thenReturn(constType);

    ExprNodeConstantDesc result = (ExprNodeConstantDesc) testSubject.interpretNodeAs(typeInfo, nodeDesc);

    assertNull(result);
  }

  @Test
  public void testWithSring() {
    testOneCase(maxValue.toString());
  }

  @Test
  public void testWithLSuffix() {
    if (intType) {
      testOneCase(maxValue.toString() + "L");
    }
  }

  @Test
  public void testWithZeroFraction() {
    if (intType) {
      testOneCase(maxValue.toString() + ".0");
    }
  }

  @Test
  public void testWithFSuffix() {
    testOneCase(maxValue.toString() + "f");
  }

  @Test
  public void testWithDSuffix() {
    testOneCase(maxValue.toString() + "D");
  }

  @Test
  public void testOverflow() {
    if (intType) {
      testNullCase(maxValue.add(BigDecimal.valueOf(1L)).toString());
    }
  }

  @Test
  public void testWithNonZeroFraction() {
    if (intType) {
      testNullCase("100.1");
    }
  }

}
