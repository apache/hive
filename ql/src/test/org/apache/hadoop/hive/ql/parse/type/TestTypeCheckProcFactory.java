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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.parse.type;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckProcFactory.DefaultExprProcessor;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for the TypeCheckProcFactory.
 */
@RunWith(Enclosed.class)
public class TestTypeCheckProcFactory {

  /**
   * Parametrized tests for constant interpretation.
   */
  @RunWith(Parameterized.class)
  public static class ConstantInterpretationTest {
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

    public ConstantInterpretationTest(String maxValue, PrimitiveTypeEntry constType, Object expectedValue,
        boolean intType) {
      this.maxValue = new BigDecimal(maxValue);
      this.constType = constType;
      this.expectedValue = expectedValue;
      this.intType = intType;
    }

    @Before
    public void init() {
      MockitoAnnotations.initMocks(this);
      testSubject = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor();
    }

    public void testOneCase(Object constValue) throws Exception {
      Mockito.when(nodeDesc.getValue()).thenReturn(constValue);
      Mockito.when(typeInfo.getPrimitiveTypeEntry()).thenReturn(constType);

      ExprNodeConstantDesc result = (ExprNodeConstantDesc) testSubject.interpretNodeAsConstant(typeInfo, nodeDesc);

      Assert.assertNotNull(result);
      Assert.assertEquals(expectedValue, result.getValue());
    }

    public void testNullCase(Object constValue) throws Exception {
      Mockito.when(nodeDesc.getValue()).thenReturn(constValue);
      Mockito.when(typeInfo.getPrimitiveTypeEntry()).thenReturn(constType);

      ExprNodeConstantDesc result = (ExprNodeConstantDesc) testSubject.interpretNodeAsConstant(typeInfo, nodeDesc);

      Assert.assertNull(result);
    }

    @Test
    public void testWithSring() throws Exception {
      testOneCase(maxValue.toString());
    }

    @Test
    public void testWithLSuffix() throws Exception {
      if (intType) {
        testOneCase(maxValue.toString() + "L");
      }
    }

    @Test
    public void testWithZeroFraction() throws Exception {
      if (intType) {
        testOneCase(maxValue.toString() + ".0");
      }
    }

    @Test
    public void testWithFSuffix() throws Exception {
      testOneCase(maxValue.toString() + "f");
    }

    @Test
    public void testWithDSuffix() throws Exception {
      testOneCase(maxValue.toString() + "D");
    }

    @Test
    public void testOverflow() throws Exception {
      if (intType) {
        testNullCase(maxValue.add(BigDecimal.valueOf(1L)).toString());
      }
    }

    @Test
    public void testWithNonZeroFraction() throws Exception {
      if (intType) {
        testNullCase("100.1");
      }
    }
  }

  /**
   * Tests for checkAmbiguousName (HIVE-29580). The planner-side call sites are covered by the
   * ambiguous_col* qtests.
   */
  public static class CheckAmbiguousNameTest {

    private static ColumnInfo colInfo() {
      ColumnInfo colInfo = new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "t", false);
      colInfo.setAlias("c");
      return colInfo;
    }

    @Test
    public void testCheckAmbiguousNameThrows() {
      ColumnInfo marked = colInfo();
      marked.setAmbiguousName(true);
      SemanticException e = Assert.assertThrows(SemanticException.class,
          () -> TypeCheckProcFactory.checkAmbiguousName(marked));
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Ambiguous column reference c in t"));
    }

    @Test
    public void testCheckAmbiguousNameNoThrow() throws SemanticException {
      TypeCheckProcFactory.checkAmbiguousName(colInfo());
    }

    @Test
    public void testCheckAmbiguousNameNullTolerant() throws SemanticException {
      TypeCheckProcFactory.checkAmbiguousName(null);
    }
  }
}
