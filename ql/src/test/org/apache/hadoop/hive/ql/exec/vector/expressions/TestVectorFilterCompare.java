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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;

import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorFilterCompare {

  public TestVectorFilterCompare() {
    // Arithmetic operations rely on getting conf from SessionState, need to initialize here.
    SessionState ss = new SessionState(new HiveConf());
    ss.getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
    SessionState.setCurrentSessionState(ss);
  }

  @Test
  public void testIntegers() throws Exception {
    Random random = new Random(7743);

    doIntegerTests(random);
  }

  @Test
  public void testIntegerFloating() throws Exception {
    Random random = new Random(7743);

    doIntegerFloatingTests(random);
  }

  @Test
  public void testFloating() throws Exception {
    Random random = new Random(7743);

    doFloatingTests(random);
  }

  @Test
  public void testDecimal() throws Exception {
    Random random = new Random(7743);

    doDecimalTests(random, /* tryDecimal64 */ false);
  }

  @Test
  public void testDecimal64() throws Exception {
    Random random = new Random(7743);

    doDecimalTests(random, /* tryDecimal64 */ true);
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(7743);

    doTests(random, TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.timestampTypeInfo);

    doTests(random, TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.longTypeInfo);
    doTests(random, TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.doubleTypeInfo);

    doTests(random, TypeInfoFactory.longTypeInfo, TypeInfoFactory.timestampTypeInfo);
    doTests(random, TypeInfoFactory.doubleTypeInfo, TypeInfoFactory.timestampTypeInfo);
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(7743);

    doTests(random, TypeInfoFactory.dateTypeInfo, TypeInfoFactory.dateTypeInfo);
  }

  @Test
  public void testInterval() throws Exception {
    Random random = new Random(7743);

    doTests(random, TypeInfoFactory.intervalYearMonthTypeInfo, TypeInfoFactory.intervalYearMonthTypeInfo);
    doTests(random, TypeInfoFactory.intervalDayTimeTypeInfo, TypeInfoFactory.intervalDayTimeTypeInfo);
  }

  @Test
  public void testStringFamily() throws Exception {
    Random random = new Random(7743);

    doTests(random, TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    doTests(random, new CharTypeInfo(10), new CharTypeInfo(10));
    doTests(random, new VarcharTypeInfo(10), new VarcharTypeInfo(10));
  }

  public enum FilterCompareTestMode {
    ROW_MODE,
    ADAPTOR,
    FILTER_VECTOR_EXPRESSION,
    COMPARE_VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN;

    static final int count = values().length;
  }

  private static TypeInfo[] integerTypeInfos = new TypeInfo[] {
    TypeInfoFactory.byteTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.intTypeInfo,
    TypeInfoFactory.longTypeInfo
  };

  // We have test failures with FLOAT.  Ignoring this issue for now.
  private static TypeInfo[] floatingTypeInfos = new TypeInfo[] {
    // TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.doubleTypeInfo
  };

  private void doIntegerTests(Random random)
          throws Exception {
    for (TypeInfo typeInfo : integerTypeInfos) {
      doTests(random, typeInfo, typeInfo);
    }
  }

  private void doIntegerFloatingTests(Random random)
      throws Exception {
    for (TypeInfo typeInfo1 : integerTypeInfos) {
      for (TypeInfo typeInfo2 : floatingTypeInfos) {
        doTests(random, typeInfo1, typeInfo2);
      }
    }
    for (TypeInfo typeInfo1 : floatingTypeInfos) {
      for (TypeInfo typeInfo2 : integerTypeInfos) {
        doTests(random, typeInfo1, typeInfo2);
      }
    }
  }

  private void doFloatingTests(Random random)
      throws Exception {
    for (TypeInfo typeInfo1 : floatingTypeInfos) {
      for (TypeInfo typeInfo2 : floatingTypeInfos) {
        doTests(random, typeInfo1, typeInfo2);
      }
    }
  }

  private static TypeInfo[] decimalTypeInfos = new TypeInfo[] {
    new DecimalTypeInfo(38, 18),
    new DecimalTypeInfo(25, 2),
    new DecimalTypeInfo(19, 4),
    new DecimalTypeInfo(18, 10),
    new DecimalTypeInfo(17, 3),
    new DecimalTypeInfo(12, 2),
    new DecimalTypeInfo(7, 1)
  };

  private void doDecimalTests(Random random, boolean tryDecimal64)
      throws Exception {
    for (TypeInfo typeInfo : decimalTypeInfos) {
      doTests(random, typeInfo, typeInfo, tryDecimal64);
    }
  }

  private TypeInfo getOutputTypeInfo(GenericUDF genericUdfClone,
      List<ObjectInspector> objectInspectorList)
    throws HiveException {

    ObjectInspector[] array =
        objectInspectorList.toArray(new ObjectInspector[objectInspectorList.size()]);
    ObjectInspector outputObjectInspector = genericUdfClone.initialize(array);
    return TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);
  }

  public enum Comparison {
    EQUALS,
    LESS_THAN,
    LESS_THAN_EQUAL,
    GREATER_THAN,
    GREATER_THAN_EQUAL,
    NOT_EQUALS;
  }

  private TypeInfo getDecimalScalarTypeInfo(Object scalarObject) {
    HiveDecimal dec = (HiveDecimal) scalarObject;
    int precision = dec.precision();
    int scale = dec.scale();
    return new DecimalTypeInfo(precision, scale);
  }

  private boolean checkDecimal64(boolean tryDecimal64, TypeInfo typeInfo) {
    if (!tryDecimal64 || !(typeInfo instanceof DecimalTypeInfo)) {
      return false;
    }
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
    boolean result = HiveDecimalWritable.isPrecisionDecimal64(decimalTypeInfo.getPrecision());
    return result;
  }

  private void doTests(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2, boolean tryDecimal64)
      throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doTestsWithDiffColumnScalar(
          random, typeInfo1, typeInfo2, columnScalarMode, tryDecimal64);
    }
  }

  private void doTests(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2)
      throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doTestsWithDiffColumnScalar(
          random, typeInfo1, typeInfo2, columnScalarMode);
    }
  }

  private void doTestsWithDiffColumnScalar(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2,
      ColumnScalarMode columnScalarMode)
          throws Exception {
    doTestsWithDiffColumnScalar(random, typeInfo1, typeInfo2, columnScalarMode, false);
  }

  private void doTestsWithDiffColumnScalar(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2,
      ColumnScalarMode columnScalarMode, boolean tryDecimal64)
          throws Exception {
    for (Comparison comparison : Comparison.values()) {
      doTestsWithDiffColumnScalar(
          random, typeInfo1, typeInfo2, columnScalarMode, comparison, tryDecimal64);
    }
  }

  private void doTestsWithDiffColumnScalar(Random random, TypeInfo typeInfo1, TypeInfo typeInfo2,
      ColumnScalarMode columnScalarMode, Comparison comparison, boolean tryDecimal64)
          throws Exception {

    String typeName1 = typeInfo1.getTypeName();
    PrimitiveCategory primitiveCategory1 =
        ((PrimitiveTypeInfo) typeInfo1).getPrimitiveCategory();

    String typeName2 = typeInfo2.getTypeName();
    PrimitiveCategory primitiveCategory2 =
        ((PrimitiveTypeInfo) typeInfo2).getPrimitiveCategory();

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 1;

    ExprNodeDesc col1Expr;
    Object scalar1Object = null;
    final boolean decimal64Enable1 = checkDecimal64(tryDecimal64, typeInfo1);
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      generationSpecList.add(
          GenerationSpec.createSameType(typeInfo1));
      explicitDataTypePhysicalVariationList.add(
          decimal64Enable1 ?
              DataTypePhysicalVariation.DECIMAL_64 :
              DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col1Expr = new ExprNodeColumnDesc(typeInfo1, columnName, "table", false);
      columns.add(columnName);
    } else {
      scalar1Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) typeInfo1);

      // Adjust the decimal type to the scalar's type...
      if (typeInfo1 instanceof DecimalTypeInfo) {
        typeInfo1 = getDecimalScalarTypeInfo(scalar1Object);
      }

      col1Expr = new ExprNodeConstantDesc(typeInfo1, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    Object scalar2Object = null;
    final boolean decimal64Enable2 = checkDecimal64(tryDecimal64, typeInfo2);
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      generationSpecList.add(
          GenerationSpec.createSameType(typeInfo2));

      explicitDataTypePhysicalVariationList.add(
          decimal64Enable2 ?
              DataTypePhysicalVariation.DECIMAL_64 :
              DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(typeInfo2, columnName, "table", false);
      columns.add(columnName);
    } else {
      scalar2Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) typeInfo2);

      // Adjust the decimal type to the scalar's type...
      if (typeInfo2 instanceof DecimalTypeInfo) {
        typeInfo2 = getDecimalScalarTypeInfo(scalar2Object);
      }

      col2Expr = new ExprNodeConstantDesc(typeInfo2, scalar2Object);
    }

    List<ObjectInspector> objectInspectorList = new ArrayList<ObjectInspector>();
    objectInspectorList.add(VectorRandomRowSource.getObjectInspector(typeInfo1));
    objectInspectorList.add(VectorRandomRowSource.getObjectInspector(typeInfo2));

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    GenericUDF genericUdf;
    switch (comparison) {
    case EQUALS:
      genericUdf = new GenericUDFOPEqual();
      break;
    case LESS_THAN:
      genericUdf = new GenericUDFOPLessThan();
      break;
    case LESS_THAN_EQUAL:
      genericUdf = new GenericUDFOPEqualOrLessThan();
      break;
    case GREATER_THAN:
      genericUdf = new GenericUDFOPGreaterThan();
      break;
    case GREATER_THAN_EQUAL:
      genericUdf = new GenericUDFOPEqualOrGreaterThan();
      break;
    case NOT_EQUALS:
      genericUdf = new GenericUDFOPNotEqual();
      break;
    default:
      throw new RuntimeException("Unexpected arithmetic " + comparison);
    }

    ObjectInspector[] objectInspectors =
        objectInspectorList.toArray(new ObjectInspector[objectInspectorList.size()]);
    ObjectInspector outputObjectInspector = null;
    try {
      outputObjectInspector = genericUdf.initialize(objectInspectors);
    } catch (Exception e) {
      Assert.fail(e.toString());
    }

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(outputTypeInfo, genericUdf, children);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[FilterCompareTestMode.count][];
    for (int i = 0; i < FilterCompareTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      FilterCompareTestMode filterCompareTestMode = FilterCompareTestMode.values()[i];
      switch (filterCompareTestMode) {
      case ROW_MODE:
        doRowFilterCompareTest(
            typeInfo1,
            typeInfo2,
            columns,
            children,
            exprDesc,
            comparison,
            randomRows,
            columnScalarMode,
            rowSource.rowStructObjectInspector(),
            outputTypeInfo,
            resultObjects);
        break;
      case ADAPTOR:
      case FILTER_VECTOR_EXPRESSION:
      case COMPARE_VECTOR_EXPRESSION:
        doVectorFilterCompareTest(
            typeInfo1,
            typeInfo2,
            columns,
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            children,
            exprDesc,
            comparison,
            filterCompareTestMode,
            columnScalarMode,
            batchSource,
            exprDesc.getWritableObjectInspector(),
            outputTypeInfo,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + filterCompareTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < FilterCompareTestMode.count; v++) {
        FilterCompareTestMode filterCompareTestMode = FilterCompareTestMode.values()[v];
        Object vectorResult = resultObjectsArray[v][i];
        if (filterCompareTestMode == FilterCompareTestMode.FILTER_VECTOR_EXPRESSION &&
            expectedResult == null &&
            vectorResult != null) {
          // This is OK.
          boolean vectorBoolean = ((BooleanWritable) vectorResult).get();
          if (vectorBoolean) {
            Assert.fail(
                "Row " + i +
                " typeName1 " + typeName1 +
                " typeName2 " + typeName2 +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + comparison +
                " " + filterCompareTestMode +
                " " + columnScalarMode +
                " result is NOT NULL and true" +
                " does not match row-mode expected result is NULL which means false here" +
                (columnScalarMode == ColumnScalarMode.SCALAR_COLUMN ?
                    " scalar1 " + scalar1Object.toString() : "") +
                " row values " + Arrays.toString(randomRows[i]) +
                (columnScalarMode == ColumnScalarMode.COLUMN_SCALAR ?
                    " scalar2 " + scalar2Object.toString() : ""));
          }
        } else if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " typeName1 " + typeName1 +
                " typeName2 " + typeName2 +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + comparison +
                " " + filterCompareTestMode +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                (columnScalarMode == ColumnScalarMode.SCALAR_COLUMN ?
                    " scalar1 " + scalar1Object.toString() : "") +
                " row values " + Arrays.toString(randomRows[i]) +
                (columnScalarMode == ColumnScalarMode.COLUMN_SCALAR ?
                    " scalar2 " + scalar2Object.toString() : ""));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " typeName1 " + typeName1 +
                " typeName2 " + typeName2 +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + comparison +
                " " + filterCompareTestMode +
                " " + columnScalarMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                (columnScalarMode == ColumnScalarMode.SCALAR_COLUMN ?
                    " scalar1 " + scalar1Object.toString() : "") +
                " row values " + Arrays.toString(randomRows[i]) +
                (columnScalarMode == ColumnScalarMode.COLUMN_SCALAR ?
                    " scalar2 " + scalar2Object.toString() : ""));
          }
        }
      }
    }
  }

  private void doRowFilterCompareTest(TypeInfo typeInfo1,
      TypeInfo typeInfo2,
      List<String> columns, List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      Comparison comparison,
      Object[][] randomRows, ColumnScalarMode columnScalarMode,
      ObjectInspector rowInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects) throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo1.toString() +
        " typeInfo2 " + typeInfo2 +
        " filterCompareTestMode ROW_MODE" +
        " columnScalarMode " + columnScalarMode +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            outputTypeInfo);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult = null;
      try {
        copyResult =
            ObjectInspectorUtils.copyToStandardObject(
                result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      } catch (Exception e) {
        Assert.fail(e.toString());
      }
      resultObjects[i] = copyResult;
    }
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow,
      ObjectInspector objectInspector, Object[] resultObjects) {

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);

      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              scrqtchRow[0], objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[rowIndex++] = copyResult;
    }
  }

  private void doVectorFilterCompareTest(TypeInfo typeInfo1,
      TypeInfo typeInfo2,
      List<String> columns,
      String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      ExprNodeGenericFuncDesc exprDesc,
      Comparison comparison,
      FilterCompareTestMode filterCompareTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (filterCompareTestMode == FilterCompareTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);

      // Don't use DECIMAL_64 with the VectorUDFAdaptor.
      dataTypePhysicalVariations = null;
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            dataTypePhysicalVariations == null ? null : Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    final VectorExpressionDescriptor.Mode mode;
    switch (filterCompareTestMode) {
    case ADAPTOR:
    case COMPARE_VECTOR_EXPRESSION:
      mode = VectorExpressionDescriptor.Mode.PROJECTION;
      break;
    case FILTER_VECTOR_EXPRESSION:
      mode = VectorExpressionDescriptor.Mode.FILTER;
      break;
    default:
      throw new RuntimeException("Unexpected filter compare mode " + filterCompareTestMode);
    }
    VectorExpression vectorExpression =
        vectorizationContext.getVectorExpression(
            exprDesc, mode);
    vectorExpression.transientInit(hiveConf);

    if (filterCompareTestMode == FilterCompareTestMode.COMPARE_VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo1 " + typeInfo1.toString() +
          " typeInfo2 " + typeInfo2.toString() +
          " " + comparison + " " +
          " filterCompareTestMode " + filterCompareTestMode +
          " columnScalarMode " + columnScalarMode +
          " vectorExpression " + vectorExpression.toString());
    }

    String[] outputScratchTypeNames= vectorizationContext.getScratchColumnTypeNames();
    DataTypePhysicalVariation[] outputDataTypePhysicalVariations =
        vectorizationContext.getScratchDataTypePhysicalVariations();

    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            typeInfos,
            dataTypePhysicalVariations,
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            outputScratchTypeNames,
            outputDataTypePhysicalVariations);

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    final int outputColumnNum = vectorExpression.getOutputColumnNum();
    resultVectorExtractRow.init(
        new TypeInfo[] { outputTypeInfo }, new int[] { outputColumnNum });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo1 " + typeInfo1.toString() +
        " typeInfo2 " + typeInfo2.toString() +
        " " + comparison + " " +
        " filterCompareTestMode " + filterCompareTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.toString());
    */

    final boolean isFilter = (mode == VectorExpressionDescriptor.Mode.FILTER);
    boolean copySelectedInUse = false;
    int[] copySelected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      final int originalBatchSize = batch.size;
      if (isFilter) {
        copySelectedInUse = batch.selectedInUse;
        if (batch.selectedInUse) {
          System.arraycopy(batch.selected, 0, copySelected, 0, originalBatchSize);
        }
      }

      // In filter mode, the batch size can be made smaller.
      vectorExpression.evaluate(batch);

      if (!isFilter) {
        extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
            objectInspector, resultObjects);
      } else {
        final int currentBatchSize = batch.size;
        if (copySelectedInUse && batch.selectedInUse) {
          int selectIndex = 0;
          for (int i = 0; i < originalBatchSize; i++) {
            final int originalBatchIndex = copySelected[i];
            final boolean booleanResult;
            if (selectIndex < currentBatchSize && batch.selected[selectIndex] == originalBatchIndex) {
              booleanResult = true;
              selectIndex++;
            } else {
              booleanResult = false;
            }
            resultObjects[rowIndex + i] = new BooleanWritable(booleanResult);
          }
        } else if (batch.selectedInUse) {
          int selectIndex = 0;
          for (int i = 0; i < originalBatchSize; i++) {
            final boolean booleanResult;
            if (selectIndex < currentBatchSize && batch.selected[selectIndex] == i) {
              booleanResult = true;
              selectIndex++;
            } else {
              booleanResult = false;
            }
            resultObjects[rowIndex + i] = new BooleanWritable(booleanResult);
          }
        } else if (currentBatchSize == 0) {
          // Whole batch got zapped.
          for (int i = 0; i < originalBatchSize; i++) {
            resultObjects[rowIndex + i] = new BooleanWritable(false);
          }
        } else {
          // Every row kept.
          for (int i = 0; i < originalBatchSize; i++) {
            resultObjects[rowIndex + i] = new BooleanWritable(true);
          }
        }
      }
      rowIndex += originalBatchSize;
    }
  }
}
