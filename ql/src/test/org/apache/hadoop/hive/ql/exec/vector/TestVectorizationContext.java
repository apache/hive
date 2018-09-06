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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.BRoundWithNumDigitsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColAndCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColOrCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DoubleColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.DynamicValueVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprOrExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncLogWithBaseDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncLogWithBaseLongToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncPowerDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprCharScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDoubleColumnDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprLongColumnLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampColumnColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampColumnScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampScalarColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprTimestampScalarScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprVarCharScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IsNotNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColModuloLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NotCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.RoundWithNumDigitsDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsFalse;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNotNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsTrue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLTrim;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLower;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUpper;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorInBloomFilterColDynamicValue;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFUnixTimeStampTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterStringColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterLongColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterDoubleColumnInList;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFYearTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterTimestampColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterTimestampColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongColumnLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleColumnDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColUnaryMinus;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColLessDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColLessDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColGreaterStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColGreaterStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharColumnBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncBRoundDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncLnDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncRoundDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FuncSinDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColMultiplyLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColSubtractLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColUnaryMinus;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarSubtractLongColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLTrim;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMod;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFPower;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRound;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestVectorizationContext {

  @Test
  public void testVectorExpressionDescriptor() {
    VectorUDFUnixTimeStampDate v1 = new VectorUDFUnixTimeStampDate();
    VectorExpressionDescriptor.Builder builder1 = new VectorExpressionDescriptor.Builder();
    VectorExpressionDescriptor.Descriptor d1 = builder1.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1).setArgumentTypes(VectorExpressionDescriptor.ArgumentType.INT_DATE_INTERVAL_YEAR_MONTH)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
    assertTrue(d1.matches(v1.getDescriptor()));

    VectorExpressionDescriptor.Builder builder2 = new VectorExpressionDescriptor.Builder();
    VectorExpressionDescriptor.Descriptor d2 = builder2.setMode(VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2).setArgumentTypes(VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY).setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
    FilterLongColLessDoubleScalar v2 = new FilterLongColLessDoubleScalar();
    assertTrue(d2.matches(v2.getDescriptor()));

    VectorExpressionDescriptor.Builder builder3 = new VectorExpressionDescriptor.Builder();
    VectorExpressionDescriptor.Descriptor d3 = builder3.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1).setArgumentTypes(VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
    StringLower v3 = new StringLower();
    assertTrue(d3.matches(v3.getDescriptor()));

    VectorExpressionDescriptor.Builder builder4 = new VectorExpressionDescriptor.Builder();
    VectorExpressionDescriptor.Descriptor d4 = builder4.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1).setArgumentTypes(VectorExpressionDescriptor.ArgumentType.ALL_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
    StringUpper v4 = new StringUpper();
    assertTrue(d4.matches(v4.getDescriptor()));

    VectorExpressionDescriptor.Builder builder5 = new VectorExpressionDescriptor.Builder();
    VectorExpressionDescriptor.Descriptor d5 = builder5.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1).setArgumentTypes(VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
    IsNull v5 = new IsNull();
    assertTrue(d5.matches(v5.getDescriptor()));
  }

  @Test
  public void testArithmeticExpressionVectorization() throws HiveException {
    /**
     * Create original expression tree for following
     * (plus (minus (plus col1 col2) col3) (multiply col4 (mod col5 col6)) )
     */
    GenericUDFOPPlus udf1 = new GenericUDFOPPlus();
    GenericUDFOPMinus udf2 = new GenericUDFOPMinus();
    GenericUDFOPMultiply udf3 = new GenericUDFOPMultiply();
    GenericUDFOPPlus udf4 = new GenericUDFOPPlus();
    GenericUDFOPMod udf5 = new GenericUDFOPMod();

    ExprNodeGenericFuncDesc sumExpr = new ExprNodeGenericFuncDesc();
    sumExpr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    sumExpr.setGenericUDF(udf1);
    ExprNodeGenericFuncDesc minusExpr = new ExprNodeGenericFuncDesc();
    minusExpr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    minusExpr.setGenericUDF(udf2);
    ExprNodeGenericFuncDesc multiplyExpr = new ExprNodeGenericFuncDesc();
    multiplyExpr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    multiplyExpr.setGenericUDF(udf3);
    ExprNodeGenericFuncDesc sum2Expr = new ExprNodeGenericFuncDesc();
    sum2Expr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    sum2Expr.setGenericUDF(udf4);
    ExprNodeGenericFuncDesc modExpr = new ExprNodeGenericFuncDesc();
    modExpr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    modExpr.setGenericUDF(udf5);

    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Long.class, "col1", "table", false);
    ExprNodeColumnDesc col2Expr = new  ExprNodeColumnDesc(Long.class, "col2", "table", false);
    ExprNodeColumnDesc col3Expr = new  ExprNodeColumnDesc(Long.class, "col3", "table", false);
    ExprNodeColumnDesc col4Expr = new  ExprNodeColumnDesc(Long.class, "col4", "table", false);
    ExprNodeColumnDesc col5Expr = new  ExprNodeColumnDesc(Long.class, "col5", "table", false);
    ExprNodeColumnDesc col6Expr = new  ExprNodeColumnDesc(Long.class, "col6", "table", false);

    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    List<ExprNodeDesc> children5 = new ArrayList<ExprNodeDesc>(2);

    children1.add(minusExpr);
    children1.add(multiplyExpr);
    sumExpr.setChildren(children1);

    children2.add(sum2Expr);
    children2.add(col3Expr);
    minusExpr.setChildren(children2);

    children3.add(col1Expr);
    children3.add(col2Expr);
    sum2Expr.setChildren(children3);

    children4.add(col4Expr);
    children4.add(modExpr);
    multiplyExpr.setChildren(children4);

    children5.add(col5Expr);
    children5.add(col6Expr);
    modExpr.setChildren(children5);

    VectorizationContext vc = new VectorizationContext("name");
    vc.addInitialColumn("col1");
    vc.addInitialColumn("col2");
    vc.addInitialColumn("col3");
    vc.addInitialColumn("col4");
    vc.addInitialColumn("col5");
    vc.addInitialColumn("col6");
    vc.finishedAddingInitialColumns();

    //Generate vectorized expression
    VectorExpression ve = vc.getVectorExpression(sumExpr, VectorExpressionDescriptor.Mode.PROJECTION);

    //Verify vectorized expression
    assertTrue(ve instanceof LongColAddLongColumn);
    assertEquals(2, ve.getChildExpressions().length);
    VectorExpression childExpr1 = ve.getChildExpressions()[0];
    VectorExpression childExpr2 = ve.getChildExpressions()[1];
    System.out.println(ve.toString());
    assertEquals(6, ve.getOutputColumnNum());

    assertTrue(childExpr1 instanceof LongColSubtractLongColumn);
    assertEquals(1, childExpr1.getChildExpressions().length);
    assertTrue(childExpr1.getChildExpressions()[0] instanceof LongColAddLongColumn);
    assertEquals(7, childExpr1.getOutputColumnNum());
    assertEquals(6, childExpr1.getChildExpressions()[0].getOutputColumnNum());

    assertTrue(childExpr2 instanceof LongColMultiplyLongColumn);
    assertEquals(1, childExpr2.getChildExpressions().length);
    assertTrue(childExpr2.getChildExpressions()[0] instanceof LongColModuloLongColumn);
    assertEquals(8, childExpr2.getOutputColumnNum());
    assertEquals(6, childExpr2.getChildExpressions()[0].getOutputColumnNum());
  }

  @Test
  public void testStringFilterExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc("Alpha");

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    exprDesc.setChildren(children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringScalar);
  }

  @Test
  public void testFilterStringColCompareStringColumnExpressions() throws HiveException {
    // Strings test
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeColumnDesc col2Expr = new  ExprNodeColumnDesc(String.class, "col2", "table", false);

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);

    // 2 CHAR test
    CharTypeInfo charTypeInfo = new CharTypeInfo(10);
    col1Expr = new  ExprNodeColumnDesc(charTypeInfo, "col1", "table", false);
    col2Expr = new  ExprNodeColumnDesc(charTypeInfo, "col2", "table", false);

    udf = new GenericUDFOPGreaterThan();
    exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    vc = new VectorizationContext("name", columns);

    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);

    // 2 VARCHAR test
    VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(10);
    col1Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col1", "table", false);
    col2Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col2", "table", false);

    udf = new GenericUDFOPGreaterThan();
    exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    vc = new VectorizationContext("name", columns);

    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);

    // Some mix tests (STRING, CHAR), (VARCHAR, CHAR), (VARCHAR, STRING)...
    col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    col2Expr = new  ExprNodeColumnDesc(charTypeInfo, "col2", "table", false);

    udf = new GenericUDFOPGreaterThan();
    exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    vc = new VectorizationContext("name", columns);

    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);

    col1Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col1", "table", false);
    col2Expr = new  ExprNodeColumnDesc(charTypeInfo, "col2", "table", false);

    udf = new GenericUDFOPGreaterThan();
    exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    vc = new VectorizationContext("name", columns);

    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);

    col1Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col1", "table", false);
    col2Expr = new  ExprNodeColumnDesc(String.class, "col2", "table", false);

    udf = new GenericUDFOPGreaterThan();
    exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(col2Expr);
    exprDesc.setChildren(children1);

    vc = new VectorizationContext("name", columns);

    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterStringGroupColGreaterStringGroupColumn);
  }

  @Test
  public void testFloatInExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Float.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPPlus udf = new GenericUDFOPPlus();

    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.doubleTypeInfo, udf,
        children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);

    assertTrue(ve.getOutputTypeInfo().equals(TypeInfoFactory.doubleTypeInfo));
  }

  @Test
  public void testVectorizeFilterAndOrExpression() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeColumnDesc col2Expr = new ExprNodeColumnDesc(Float.class, "col2", "table", false);
    ExprNodeConstantDesc const2Desc = new ExprNodeConstantDesc(new Float(1.0));

    GenericUDFOPLessThan udf2 = new GenericUDFOPLessThan();
    ExprNodeGenericFuncDesc lessExprDesc = new ExprNodeGenericFuncDesc();
    lessExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    lessExprDesc.setGenericUDF(udf2);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>(2);
    children2.add(col2Expr);
    children2.add(const2Desc);
    lessExprDesc.setChildren(children2);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(greaterExprDesc);
    children3.add(lessExprDesc);
    andExprDesc.setChildren(children3);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(andExprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertEquals(ve.getClass(), FilterExprAndExpr.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(ve.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);

    GenericUDFOPOr orUdf = new GenericUDFOPOr();
    ExprNodeGenericFuncDesc orExprDesc = new ExprNodeGenericFuncDesc();
    orExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    orExprDesc.setGenericUDF(orUdf);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    children4.add(greaterExprDesc);
    children4.add(lessExprDesc);
    orExprDesc.setChildren(children4);
    VectorExpression veOr = vc.getVectorExpression(orExprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(veOr.getClass(), FilterExprOrExpr.class);
    assertEquals(veOr.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(veOr.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);
  }

  @Test
  public void testVectorizeFilterMultiAndOrExpression() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeColumnDesc col2Expr = new ExprNodeColumnDesc(Float.class, "col2", "table", false);
    ExprNodeConstantDesc const2Desc = new ExprNodeConstantDesc(new Float(1.0));

    GenericUDFOPLessThan udf2 = new GenericUDFOPLessThan();
    ExprNodeGenericFuncDesc lessExprDesc = new ExprNodeGenericFuncDesc();
    lessExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    lessExprDesc.setGenericUDF(udf2);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>(2);
    children2.add(col2Expr);
    children2.add(const2Desc);
    lessExprDesc.setChildren(children2);

    ExprNodeColumnDesc col3Expr = new ExprNodeColumnDesc(Integer.class, "col3", "table", false);
    ExprNodeConstantDesc const3Desc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf3 = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc3 = new ExprNodeGenericFuncDesc();
    greaterExprDesc3.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc3.setGenericUDF(udf3);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(col3Expr);
    children3.add(const3Desc);
    greaterExprDesc3.setChildren(children3);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    children4.add(greaterExprDesc);
    children4.add(lessExprDesc);
    children4.add(greaterExprDesc3);
    andExprDesc.setChildren(children4);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    columns.add("col3");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(andExprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertEquals(ve.getClass(), FilterExprAndExpr.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(ve.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);
    assertEquals(ve.getChildExpressions()[2].getClass(), FilterLongColGreaterLongScalar.class);

    GenericUDFOPOr orUdf = new GenericUDFOPOr();
    ExprNodeGenericFuncDesc orExprDesc = new ExprNodeGenericFuncDesc();
    orExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    orExprDesc.setGenericUDF(orUdf);
    List<ExprNodeDesc> children5 = new ArrayList<ExprNodeDesc>(2);
    children5.add(greaterExprDesc);
    children5.add(lessExprDesc);
    children5.add(greaterExprDesc3);
    orExprDesc.setChildren(children5);
    VectorExpression veOr = vc.getVectorExpression(orExprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(veOr.getClass(), FilterExprOrExpr.class);
    assertEquals(veOr.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(veOr.getChildExpressions()[1].getClass(), FilterDoubleColLessDoubleScalar.class);
    assertEquals(ve.getChildExpressions()[2].getClass(), FilterLongColGreaterLongScalar.class);
  }

  @Test
  public void testVectorizeAndOrProjectionExpression() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeColumnDesc col2Expr = new ExprNodeColumnDesc(Boolean.class, "col2", "table", false);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(greaterExprDesc);
    children3.add(col2Expr);
    andExprDesc.setChildren(children3);

    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression veAnd = vc.getVectorExpression(andExprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(veAnd.getClass(), FilterExprAndExpr.class);
    assertEquals(veAnd.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(veAnd.getChildExpressions()[1].getClass(), SelectColumnIsTrue.class);

    veAnd = vc.getVectorExpression(andExprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(veAnd.getClass(), ColAndCol.class);
    assertEquals(1, veAnd.getChildExpressions().length);
    assertEquals(veAnd.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
    assertEquals(3, ((ColAndCol) veAnd).getOutputColumnNum());

    //OR
    GenericUDFOPOr orUdf = new GenericUDFOPOr();
    ExprNodeGenericFuncDesc orExprDesc = new ExprNodeGenericFuncDesc();
    orExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    orExprDesc.setGenericUDF(orUdf);
    List<ExprNodeDesc> children4 = new ArrayList<ExprNodeDesc>(2);
    children4.add(greaterExprDesc);
    children4.add(col2Expr);
    orExprDesc.setChildren(children4);

    //Allocate new Vectorization context to reset the intermediate columns.
    vc = new VectorizationContext("name", columns);
    VectorExpression veOr = vc.getVectorExpression(orExprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(veOr.getClass(), FilterExprOrExpr.class);
    assertEquals(veOr.getChildExpressions()[0].getClass(), FilterLongColGreaterLongScalar.class);
    assertEquals(veOr.getChildExpressions()[1].getClass(), SelectColumnIsTrue.class);

    veOr = vc.getVectorExpression(orExprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(veOr.getClass(), ColOrCol.class);
    assertEquals(1, veAnd.getChildExpressions().length);
    assertEquals(veAnd.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
    assertEquals(3, ((ColOrCol) veOr).getOutputColumnNum());
  }

  @Test
  public void testNotExpression() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeGenericFuncDesc notExpr = new ExprNodeGenericFuncDesc();
    notExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    GenericUDFOPNot notUdf = new GenericUDFOPNot();
    notExpr.setGenericUDF(notUdf);
    List<ExprNodeDesc> childOfNot = new ArrayList<ExprNodeDesc>();
    childOfNot.add(greaterExprDesc);
    notExpr.setChildren(childOfNot);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(notExpr, VectorExpressionDescriptor.Mode.FILTER);

    assertEquals(ve.getClass(), SelectColumnIsFalse.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);

    ve = vc.getVectorExpression(notExpr, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(ve.getClass(), NotCol.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
  }

  @Test
  public void testNullExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeGenericFuncDesc isNullExpr = new ExprNodeGenericFuncDesc();
    isNullExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    GenericUDFOPNull isNullUdf = new GenericUDFOPNull();
    isNullExpr.setGenericUDF(isNullUdf);
    List<ExprNodeDesc> childOfIsNull = new ArrayList<ExprNodeDesc>();
    childOfIsNull.add(greaterExprDesc);
    isNullExpr.setChildren(childOfIsNull);

    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(isNullExpr, VectorExpressionDescriptor.Mode.FILTER);

    assertEquals(ve.getClass(), SelectColumnIsNull.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
    assertEquals(2, ve.getChildExpressions()[0].getOutputColumnNum());

    ve = vc.getVectorExpression(isNullExpr, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(ve.getClass(), IsNull.class);
    assertEquals(3, ve.getOutputColumnNum());
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
  }

  @Test
  public void testNotNullExpressions() throws HiveException {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    ExprNodeGenericFuncDesc isNotNullExpr = new ExprNodeGenericFuncDesc();
    isNotNullExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    GenericUDFOPNotNull notNullUdf = new GenericUDFOPNotNull();
    isNotNullExpr.setGenericUDF(notNullUdf);
    List<ExprNodeDesc> childOfNot = new ArrayList<ExprNodeDesc>();
    childOfNot.add(greaterExprDesc);
    isNotNullExpr.setChildren(childOfNot);

    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(isNotNullExpr, VectorExpressionDescriptor.Mode.FILTER);

    assertEquals(ve.getClass(), SelectColumnIsNotNull.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);

    ve = vc.getVectorExpression(isNotNullExpr, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(ve.getClass(), IsNotNull.class);
    assertEquals(ve.getChildExpressions()[0].getClass(), LongColGreaterLongScalar.class);
  }

  @Test
  public void testVectorizeScalarColumnExpression() throws HiveException {
    GenericUDFOPMinus gudf = new GenericUDFOPMinus();
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 20);
    ExprNodeColumnDesc colDesc = new ExprNodeColumnDesc(Long.class, "a", "table", false);

    children.add(constDesc);
    children.add(colDesc);

    ExprNodeGenericFuncDesc scalarMinusConstant = new ExprNodeGenericFuncDesc(TypeInfoFactory.longTypeInfo,
        gudf, children);

    List<String> columns = new ArrayList<String>();
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(scalarMinusConstant, VectorExpressionDescriptor.Mode.PROJECTION);

    assertEquals(ve.getClass(), LongScalarSubtractLongColumn.class);
  }

  @Test
  public void testFilterWithNegativeScalar() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(new Integer(-10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc();
    exprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    exprDesc.setChildren(children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);

    assertTrue(ve instanceof FilterLongColGreaterLongScalar);
  }

  @Test
  public void testUnaryMinusColumnLong() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    GenericUDF gudf = new GenericUDFOPNegative();
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(1);
    children.add(col1Expr);
    ExprNodeGenericFuncDesc negExprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.longTypeInfo, gudf,
        children);
    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(negExprDesc, VectorExpressionDescriptor.Mode.PROJECTION);

    assertTrue( ve instanceof LongColUnaryMinus);
  }

  @Test
  public void testUnaryMinusColumnDouble() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Float.class, "col1", "table", false);
    GenericUDF gudf = new GenericUDFOPNegative();
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(1);
    children.add(col1Expr);
    ExprNodeGenericFuncDesc negExprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.doubleTypeInfo, gudf,
        children);
    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(negExprDesc, VectorExpressionDescriptor.Mode.PROJECTION);

    assertTrue( ve instanceof DoubleColUnaryMinus);
  }

  @Test
  public void testFilterScalarCompareColumn() throws HiveException {
    ExprNodeGenericFuncDesc scalarGreaterColExpr = new ExprNodeGenericFuncDesc();
    GenericUDFOPGreaterThan gudf = new GenericUDFOPGreaterThan();
    scalarGreaterColExpr.setGenericUDF(gudf);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    ExprNodeConstantDesc constDesc =
        new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 20);
    ExprNodeColumnDesc colDesc =
        new ExprNodeColumnDesc(Long.class, "a", "table", false);

    children.add(constDesc);
    children.add(colDesc);

    scalarGreaterColExpr.setChildren(children);

    List<String> columns = new ArrayList<String>();
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(scalarGreaterColExpr, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(FilterLongScalarGreaterLongColumn.class, ve.getClass());
  }

  @Test
  public void testFilterBooleanColumnCompareBooleanScalar() throws HiveException {
    ExprNodeGenericFuncDesc colEqualScalar = new ExprNodeGenericFuncDesc();
    GenericUDFOPEqual gudf = new GenericUDFOPEqual();
    colEqualScalar.setGenericUDF(gudf);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    ExprNodeConstantDesc constDesc =
        new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, 20);
    ExprNodeColumnDesc colDesc =
        new ExprNodeColumnDesc(Boolean.class, "a", "table", false);

    children.add(colDesc);
    children.add(constDesc);

    colEqualScalar.setChildren(children);

    List<String> columns = new ArrayList<String>();
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(colEqualScalar, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(FilterLongColEqualLongScalar.class, ve.getClass());
  }

  @Test
  public void testBooleanColumnCompareBooleanScalar() throws HiveException {
    ExprNodeGenericFuncDesc colEqualScalar =
        new ExprNodeGenericFuncDesc();
    GenericUDFOPEqual gudf = new GenericUDFOPEqual();
    colEqualScalar.setGenericUDF(gudf);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>(2);
    ExprNodeConstantDesc constDesc =
        new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, 20);
    ExprNodeColumnDesc colDesc =
        new ExprNodeColumnDesc(Boolean.class, "a", "table", false);

    children.add(colDesc);
    children.add(constDesc);

    colEqualScalar.setChildren(children);
    colEqualScalar.setTypeInfo(TypeInfoFactory.booleanTypeInfo);

    List<String> columns = new ArrayList<String>();
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(colEqualScalar, VectorExpressionDescriptor.Mode.PROJECTION);
    assertEquals(LongColEqualLongScalar.class, ve.getClass());
  }

  @Test
  public void testUnaryStringExpressions() throws HiveException {
    ExprNodeGenericFuncDesc stringUnary = new ExprNodeGenericFuncDesc();
    stringUnary.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    ExprNodeColumnDesc colDesc = new ExprNodeColumnDesc(String.class, "a", "table", false);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(colDesc);
    stringUnary.setChildren(children);

    List<String> columns = new ArrayList<String>();
    columns.add("b");
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);
    GenericUDF stringLower = new GenericUDFLower();
    stringUnary.setGenericUDF(stringLower);

    VectorExpression ve = vc.getVectorExpression(stringUnary);

    assertEquals(StringLower.class, ve.getClass());
    assertEquals(2, ((StringLower) ve).getOutputColumnNum());

    vc = new VectorizationContext("name", columns);

    ExprNodeGenericFuncDesc anotherUnary = new ExprNodeGenericFuncDesc();
    anotherUnary.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>();
    children2.add(stringUnary);
    anotherUnary.setChildren(children2);
    GenericUDFBridge udfbridge = new GenericUDFBridge("ltrim", false, GenericUDFLTrim.class.getName());
    anotherUnary.setGenericUDF(udfbridge);

    ve = vc.getVectorExpression(anotherUnary);
    VectorExpression childVe = ve.getChildExpressions()[0];
    assertEquals(StringLower.class, childVe.getClass());
    assertEquals(2, ((StringLower) childVe).getOutputColumnNum());

    assertEquals(StringLTrim.class, ve.getClass());
    assertEquals(3, ((StringLTrim) ve).getOutputColumnNum());
  }

  @Test
  public void testMathFunctions() throws HiveException {
    ExprNodeGenericFuncDesc mathFuncExpr = new ExprNodeGenericFuncDesc();
    mathFuncExpr.setTypeInfo(TypeInfoFactory.doubleTypeInfo);
    ExprNodeColumnDesc colDesc1 = new ExprNodeColumnDesc(Integer.class, "a", "table", false);
    ExprNodeColumnDesc colDesc2 = new ExprNodeColumnDesc(Double.class, "b", "table", false);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>();
    children1.add(colDesc1);
    children2.add(colDesc2);

    List<String> columns = new ArrayList<String>();
    columns.add("b");
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);

    // Sin(double)
    GenericUDFBridge gudfBridge = new GenericUDFBridge("sin", false, UDFSin.class.getName());
    mathFuncExpr.setGenericUDF(gudfBridge);
    mathFuncExpr.setChildren(children2);
    VectorExpression ve = vc.getVectorExpression(mathFuncExpr, VectorExpressionDescriptor.Mode.PROJECTION);
    Assert.assertEquals(FuncSinDoubleToDouble.class, ve.getClass());

    // Round without digits
    GenericUDFRound udfRound = new GenericUDFRound();
    mathFuncExpr.setGenericUDF(udfRound);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncRoundDoubleToDouble.class, ve.getClass());

    // BRound without digits
    GenericUDFBRound udfBRound = new GenericUDFBRound();
    mathFuncExpr.setGenericUDF(udfBRound);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncBRoundDoubleToDouble.class, ve.getClass());

    // Round with digits
    mathFuncExpr.setGenericUDF(udfRound);
    children2.add(new ExprNodeConstantDesc(4));
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(RoundWithNumDigitsDoubleToDouble.class, ve.getClass());
    Assert.assertEquals(4, ((RoundWithNumDigitsDoubleToDouble) ve).getDecimalPlaces().get());

    // BRound with digits
    mathFuncExpr.setGenericUDF(udfBRound);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(BRoundWithNumDigitsDoubleToDouble.class, ve.getClass());
    Assert.assertEquals(4, ((BRoundWithNumDigitsDoubleToDouble) ve).getDecimalPlaces().get());

    // Logger with int base
    gudfBridge = new GenericUDFBridge("log", false, UDFLog.class.getName());
    mathFuncExpr.setGenericUDF(gudfBridge);
    children2.clear();
    children2.add(new ExprNodeConstantDesc(4.0));
    children2.add(colDesc2);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncLogWithBaseDoubleToDouble.class, ve.getClass());
    Assert.assertTrue(4 == ((FuncLogWithBaseDoubleToDouble) ve).getBase());

    // Logger with default base
    children2.clear();
    children2.add(colDesc2);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncLnDoubleToDouble.class, ve.getClass());

    //Log with double base
    children2.clear();
    children2.add(new ExprNodeConstantDesc(4.5));
    children2.add(colDesc2);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncLogWithBaseDoubleToDouble.class, ve.getClass());
    Assert.assertTrue(4.5 == ((FuncLogWithBaseDoubleToDouble) ve).getBase());

    //Log with int input and double base
    children2.clear();
    children2.add(new ExprNodeConstantDesc(4.5));
    children2.add(colDesc1);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncLogWithBaseLongToDouble.class, ve.getClass());
    Assert.assertTrue(4.5 == ((FuncLogWithBaseLongToDouble) ve).getBase());

    //Power with double power
    children2.clear();
    children2.add(colDesc2);
    children2.add(new ExprNodeConstantDesc(4.5));
    mathFuncExpr.setGenericUDF(new GenericUDFPower());
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncPowerDoubleToDouble.class, ve.getClass());
    Assert.assertTrue(4.5 == ((FuncPowerDoubleToDouble) ve).getPower());

    //Round with default decimal places
    mathFuncExpr.setGenericUDF(udfRound);
    children2.clear();
    children2.add(colDesc2);
    mathFuncExpr.setChildren(children2);
    ve = vc.getVectorExpression(mathFuncExpr);
    Assert.assertEquals(FuncRoundDoubleToDouble.class, ve.getClass());
  }

  @Test
  public void testTimeStampUdfs() throws HiveException {
    ExprNodeGenericFuncDesc tsFuncExpr = new ExprNodeGenericFuncDesc();
    tsFuncExpr.setTypeInfo(TypeInfoFactory.intTypeInfo);
    ExprNodeColumnDesc colDesc1 = new ExprNodeColumnDesc(
        TypeInfoFactory.timestampTypeInfo, "a", "table", false);
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(colDesc1);

    List<String> columns = new ArrayList<String>();
    columns.add("b");
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);

    //UDFYear
    GenericUDFBridge gudfBridge = new GenericUDFBridge("year", false, UDFYear.class.getName());
    tsFuncExpr.setGenericUDF(gudfBridge);
    tsFuncExpr.setChildren(children);
    VectorExpression ve = vc.getVectorExpression(tsFuncExpr);
    Assert.assertEquals(VectorUDFYearTimestamp.class, ve.getClass());

    //GenericUDFToUnixTimeStamp
    GenericUDFToUnixTimeStamp gudf = new GenericUDFToUnixTimeStamp();
    tsFuncExpr.setGenericUDF(gudf);
    tsFuncExpr.setTypeInfo(TypeInfoFactory.longTypeInfo);
    ve = vc.getVectorExpression(tsFuncExpr);
    Assert.assertEquals(VectorUDFUnixTimeStampTimestamp.class, ve.getClass());
  }

  @Test
  public void testBetweenFilters() throws HiveException {
    // string tests
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc("Alpha");
    ExprNodeConstantDesc constDesc2 = new ExprNodeConstantDesc("Bravo");

    // string BETWEEN
    GenericUDFBetween udf = new GenericUDFBetween();
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>();
    children1.add(new ExprNodeConstantDesc(new Boolean(false))); // no NOT keyword
    children1.add(col1Expr);
    children1.add(constDesc);
    children1.add(constDesc2);
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf,
        children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);

    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterStringColumnBetween);

    // string NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true))); // has NOT keyword
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterStringColumnNotBetween);

    // CHAR tests
    CharTypeInfo charTypeInfo = new CharTypeInfo(10);
    col1Expr = new  ExprNodeColumnDesc(charTypeInfo, "col1", "table", false);
    constDesc = new ExprNodeConstantDesc(charTypeInfo, new HiveChar("Alpha", 10));
    constDesc2 = new ExprNodeConstantDesc(charTypeInfo, new HiveChar("Bravo", 10));

    // CHAR BETWEEN
    udf = new GenericUDFBetween();
    children1 = new ArrayList<ExprNodeDesc>();
    children1.add(new ExprNodeConstantDesc(new Boolean(false))); // no NOT keyword
    children1.add(col1Expr);
    children1.add(constDesc);
    children1.add(constDesc2);
    exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf,
        children1);

    vc = new VectorizationContext("name", columns);
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterCharColumnBetween);

    // CHAR NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true))); // has NOT keyword
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterCharColumnNotBetween);

    // VARCHAR tests
    VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(10);
    col1Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col1", "table", false);
    constDesc = new ExprNodeConstantDesc(varcharTypeInfo, new HiveVarchar("Alpha", 10));
    constDesc2 = new ExprNodeConstantDesc(varcharTypeInfo, new HiveVarchar("Bravo", 10));

    // VARCHAR BETWEEN
    udf = new GenericUDFBetween();
    children1 = new ArrayList<ExprNodeDesc>();
    children1.add(new ExprNodeConstantDesc(new Boolean(false))); // no NOT keyword
    children1.add(col1Expr);
    children1.add(constDesc);
    children1.add(constDesc2);
    exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf,
        children1);

    vc = new VectorizationContext("name", columns);
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterVarCharColumnBetween);

    // VARCHAR NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true))); // has NOT keyword
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterVarCharColumnNotBetween);

    // long BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(false)));
    children1.set(1, new ExprNodeColumnDesc(Long.class, "col1", "table", false));
    children1.set(2, new ExprNodeConstantDesc(10));
    children1.set(3, new ExprNodeConstantDesc(20));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterLongColumnBetween);

    // long NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true)));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterLongColumnNotBetween);

    // double BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(false)));
    children1.set(1, new ExprNodeColumnDesc(Double.class, "col1", "table", false));
    children1.set(2, new ExprNodeConstantDesc(10.0d));
    children1.set(3, new ExprNodeConstantDesc(20.0d));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterDoubleColumnBetween);

    // double NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true)));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterDoubleColumnNotBetween);

    // timestamp BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(false)));
    children1.set(1, new ExprNodeColumnDesc(Timestamp.class, "col1", "table", false));
    children1.set(2, new ExprNodeConstantDesc("2013-11-05 00:00:00.000"));
    children1.set(3, new ExprNodeConstantDesc("2013-11-06 00:00:00.000"));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(FilterTimestampColumnBetween.class, ve.getClass());

    // timestamp NOT BETWEEN
    children1.set(0, new ExprNodeConstantDesc(new Boolean(true)));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertEquals(FilterTimestampColumnNotBetween.class, ve.getClass());
  }

  // Test translation of both IN filters and boolean-valued IN expressions (non-filters).
  @Test
  public void testInFiltersAndExprs() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc("Alpha");
    ExprNodeConstantDesc constDesc2 = new ExprNodeConstantDesc("Bravo");

    // string IN
    GenericUDFIn udf = new GenericUDFIn();
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>();
    children1.add(col1Expr);
    children1.add(constDesc);
    children1.add(constDesc2);
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        udf, children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterStringColumnInList);
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    assertTrue(ve instanceof StringColumnInList);

    // long IN
    children1.set(0, new ExprNodeColumnDesc(Long.class, "col1", "table", false));
    children1.set(1, new ExprNodeConstantDesc(10));
    children1.set(2, new ExprNodeConstantDesc(20));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterLongColumnInList);
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    assertTrue(ve instanceof LongColumnInList);

    // double IN
    children1.set(0, new ExprNodeColumnDesc(Double.class, "col1", "table", false));
    children1.set(1, new ExprNodeConstantDesc(10d));
    children1.set(2, new ExprNodeConstantDesc(20d));
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.FILTER);
    assertTrue(ve instanceof FilterDoubleColumnInList);
    ve = vc.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    assertTrue(ve instanceof DoubleColumnInList);
  }

  /**
   * Test that correct VectorExpression classes are chosen for the
   * IF (expr1, expr2, expr3) conditional expression for integer, float,
   * boolean, timestamp and string input types. expr1 is always an input column expression
   * of type long. expr2 and expr3 can be column expressions or constants of other types
   * but must have the same type.
   */
  @Test
  public void testIfConditionalExprs() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Long.class, "col1", "table", false);
    ExprNodeColumnDesc col2Expr = new  ExprNodeColumnDesc(Long.class, "col2", "table", false);
    ExprNodeColumnDesc col3Expr = new  ExprNodeColumnDesc(Long.class, "col3", "table", false);

    ExprNodeConstantDesc constDesc2 = new ExprNodeConstantDesc(new Integer(1));
    ExprNodeConstantDesc constDesc3 = new ExprNodeConstantDesc(new Integer(2));

    // long column/column IF
    GenericUDFIf udf = new GenericUDFIf();
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>();
    children1.add(col1Expr);
    children1.add(col2Expr);
    children1.add(col3Expr);
    ExprNodeGenericFuncDesc exprDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf,
        children1);

    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    columns.add("col3");
    VectorizationContext vc = new VectorizationContext("name", columns);
    VectorExpression ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongColumnLongColumn);

    // long column/scalar IF
    children1.set(2,  new ExprNodeConstantDesc(1L));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongColumnLongScalar);

    // long scalar/scalar IF
    children1.set(1, new ExprNodeConstantDesc(1L));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongScalarLongScalar);

    // long scalar/column IF
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongScalarLongColumn);

    // test for double type
    col2Expr = new  ExprNodeColumnDesc(Double.class, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(Double.class, "col3", "table", false);

    // double column/column IF
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprDoubleColumnDoubleColumn);

    // double column/scalar IF
    children1.set(2,  new ExprNodeConstantDesc(1D));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprDoubleColumnDoubleScalar);

    // double scalar/scalar IF
    children1.set(1, new ExprNodeConstantDesc(1D));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprDoubleScalarDoubleScalar);

    // double scalar/column IF
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprDoubleScalarDoubleColumn);

    // double scalar/long column IF
    children1.set(2, new  ExprNodeColumnDesc(Long.class, "col3", "table", false));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprDoubleScalarLongColumn);

    // Additional combinations of (long,double)X(column,scalar) for each of the second
    // and third arguments are omitted. We have coverage of all the source templates
    // already.

    // test for timestamp type
    col2Expr = new  ExprNodeColumnDesc(Timestamp.class, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(Timestamp.class, "col3", "table", false);

    // timestamp column/column IF
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprTimestampColumnColumn);

    // timestamp column/scalar IF where scalar is really a CAST of a constant to timestamp.
    ExprNodeGenericFuncDesc f = new ExprNodeGenericFuncDesc();
    f.setGenericUDF(new GenericUDFTimestamp());
    f.setTypeInfo(TypeInfoFactory.timestampTypeInfo);
    List<ExprNodeDesc> children2 = new ArrayList<ExprNodeDesc>();
    f.setChildren(children2);
    children2.add(new ExprNodeConstantDesc("2013-11-05 00:00:00.000"));
    children1.set(2, f);
    ve = vc.getVectorExpression(exprDesc);

    // We check for two different classes below because initially the result
    // is IfExprLongColumnLongColumn but in the future if the system is enhanced
    // with constant folding then the result will be IfExprLongColumnLongScalar.
    assertTrue(IfExprTimestampColumnColumn.class == ve.getClass()
               || IfExprTimestampColumnScalar.class == ve.getClass());

    // timestamp scalar/scalar
    children1.set(1, f);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(IfExprTimestampColumnColumn.class == ve.getClass()
        || IfExprTimestampScalarScalar.class == ve.getClass());

    // timestamp scalar/column
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(IfExprTimestampColumnColumn.class == ve.getClass()
        || IfExprTimestampScalarColumn.class == ve.getClass());

    // test for boolean type
    col2Expr = new  ExprNodeColumnDesc(Boolean.class, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(Boolean.class, "col3", "table", false);

    // column/column
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongColumnLongColumn);

    // column/scalar IF
    children1.set(2,  new ExprNodeConstantDesc(true));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongColumnLongScalar);

    // scalar/scalar IF
    children1.set(1, new ExprNodeConstantDesc(true));
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongScalarLongScalar);

    // scalar/column IF
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprLongScalarLongColumn);

    // test for string type
    constDesc2 = new ExprNodeConstantDesc("Alpha");
    constDesc3 = new ExprNodeConstantDesc("Bravo");
    col2Expr = new  ExprNodeColumnDesc(String.class, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(String.class, "col3", "table", false);

    // column/column
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnStringGroupColumn);

    // column/scalar
    children1.set(2,  constDesc3);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnStringScalar);

    // scalar/scalar
    children1.set(1,  constDesc2);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringScalarStringScalar);

    // scalar/column
    children1.set(2,  col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringScalarStringGroupColumn);

    // test for CHAR type
    CharTypeInfo charTypeInfo = new CharTypeInfo(10);
    constDesc2 = new ExprNodeConstantDesc(charTypeInfo, new HiveChar("Alpha", 10));
    constDesc3 = new ExprNodeConstantDesc(charTypeInfo, new HiveChar("Bravo", 10));
    col2Expr = new  ExprNodeColumnDesc(charTypeInfo, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(charTypeInfo, "col3", "table", false);

    // column/column
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnStringGroupColumn);

    // column/scalar
    children1.set(2,  constDesc3);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnCharScalar);

    // scalar/scalar
    children1.set(1,  constDesc2);
//    ve = vc.getVectorExpression(exprDesc);
//    assertTrue(ve instanceof IfExprCharScalarCharScalar);

    // scalar/column
    children1.set(2,  col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprCharScalarStringGroupColumn);

    // test for VARCHAR type
    VarcharTypeInfo varcharTypeInfo = new VarcharTypeInfo(10);
    constDesc2 = new ExprNodeConstantDesc(varcharTypeInfo, new HiveVarchar("Alpha", 10));
    constDesc3 = new ExprNodeConstantDesc(varcharTypeInfo, new HiveVarchar("Bravo", 10));
    col2Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col2", "table", false);
    col3Expr = new  ExprNodeColumnDesc(varcharTypeInfo, "col3", "table", false);

    // column/column
    children1.set(1, col2Expr);
    children1.set(2, col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnStringGroupColumn);

    // column/scalar
    children1.set(2,  constDesc3);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprStringGroupColumnVarCharScalar);

    // scalar/scalar
    children1.set(1,  constDesc2);
//    ve = vc.getVectorExpression(exprDesc);
//    assertTrue(ve instanceof IfExprVarCharScalarVarCharScalar);

    // scalar/column
    children1.set(2,  col3Expr);
    ve = vc.getVectorExpression(exprDesc);
    assertTrue(ve instanceof IfExprVarCharScalarStringGroupColumn);
  }

  @Test
  public void testSIMDEqual() {
    long a;
    long b;

    a = 0; b = 0; assertEquals(a == b ? 1 : 0, (((a - b) ^ (b - a)) >>> 63) ^ 1);
    a = 1; b = 0; assertEquals(a == b ? 1 : 0, (((a - b) ^ (b - a)) >>> 63) ^ 1);
    a = 0; b = 1; assertEquals(a == b ? 1 : 0, (((a - b) ^ (b - a)) >>> 63) ^ 1);
  }

  @Test
  public void testSIMDGreaterThan() {
    long a;
    long b;

    a = 0; b = 0; assertEquals(a > b ? 1 : 0, (b - a) >>> 63);
    a = 1; b = 0; assertEquals(a > b ? 1 : 0, (b - a) >>> 63);
    a = 0; b = 1; assertEquals(a > b ? 1 : 0, (b - a) >>> 63);
  }

  @Test
  public void testSIMDGreaterEqual() {
    long a;
    long b;

    a = 0;
    b = 0;
    assertEquals(a >= b ? 1 : 0, ((a - b) >>> 63) ^ 1);

    a = 1;
    b = 0;
    assertEquals(a >= b ? 1 : 0, ((a - b) >>> 63) ^ 1);

    a = 0;
    b = 1;
    assertEquals(a >= b ? 1 : 0, ((a - b) >>> 63) ^ 1);
  }

  @Test
  public void testSIMDLessEqual() {
    long a;
    long b;

    a = 0;
    b = 0;
    assertEquals(a <= b ? 1 : 0, ((b - a) >>> 63) ^ 1);

    a = 1;
    b = 0;
    assertEquals(a <= b ? 1 : 0, ((b - a) >>> 63) ^ 1);

    a = 0;
    b = 1;
    assertEquals(a <= b ? 1 : 0, ((b - a) >>> 63) ^ 1);
  }

  @Test
  public void testSIMDLessThan() {
    long a;
    long b;

    a = 0;
    b = 0;
    assertEquals(a < b ? 1 : 0, (a - b) >>> 63);

    a = 1;
    b = 0;
    assertEquals(a < b ? 1 : 0, (a - b) >>> 63);

    a = 0;
    b = 1;
    assertEquals(a < b ? 1 : 0, (a - b) >>> 63);
  }

  @Test
  public void testSIMDNotEqual() {
    long a;
    long b;

    a = 0;
    b = 0;
    assertEquals(a != b ? 1 : 0, ((a - b) ^ (b - a)) >>> 63);

    a = 1;
    b = 0;
    assertEquals(a != b ? 1 : 0, ((a - b) ^ (b - a)) >>> 63);

    a = 0;
    b = 1;
    assertEquals(a != b ? 1 : 0, ((a - b) ^ (b - a)) >>> 63);
  }

  @Test
  public void testInBloomFilter() throws Exception {
    // Setup InBloomFilter() UDF
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(TypeInfoFactory.getDecimalTypeInfo(10, 5), "a", "table", false);
    ExprNodeDesc bfExpr = new ExprNodeDynamicValueDesc(new DynamicValue("id1", TypeInfoFactory.binaryTypeInfo));

    ExprNodeGenericFuncDesc inBloomFilterExpr = new ExprNodeGenericFuncDesc();
    GenericUDF inBloomFilterUdf = new GenericUDFInBloomFilter();
    inBloomFilterExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    inBloomFilterExpr.setGenericUDF(inBloomFilterUdf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(colExpr);
    children1.add(bfExpr);
    inBloomFilterExpr.setChildren(children1);

    // Setup VectorizationContext
    List<String> columns = new ArrayList<String>();
    columns.add("b");
    columns.add("a");
    VectorizationContext vc = new VectorizationContext("name", columns);

    // Create vectorized expr
    VectorExpression ve = vc.getVectorExpression(inBloomFilterExpr, VectorExpressionDescriptor.Mode.FILTER);
    Assert.assertEquals(VectorInBloomFilterColDynamicValue.class, ve.getClass());
    VectorInBloomFilterColDynamicValue vectorizedInBloomFilterExpr = (VectorInBloomFilterColDynamicValue) ve;
    VectorExpression[] children = vectorizedInBloomFilterExpr.getChildExpressions();
    // VectorInBloomFilterColDynamicValue should have all of the necessary information to vectorize.
    // Should be no need for child vector expressions, which would imply casting/conversion.
    Assert.assertNull(children);
  }
}
