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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;

import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorCastStatement {

  @Test
  public void testBoolean() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "boolean");
  }

  @Test
  public void testTinyInt() throws Exception {
    Random random = new Random(5371);

    doIfTests(random, "tinyint");
  }

  @Test
  public void testSmallInt() throws Exception {
    Random random = new Random(2772);

    doIfTests(random, "smallint");
  }

  @Test
  public void testInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "int");
  }

  @Test
  public void testBigInt() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "bigint");
  }

  @Test
  public void testString() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "string");
  }

  @Test
  public void testTimestamp() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "timestamp");
  }

  @Test
  public void testDate() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "date");
  }

  @Test
  public void testFloat() throws Exception {
    Random random = new Random(7322);

    doIfTests(random, "float");
  }

  @Test
  public void testDouble() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "double");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doIfTests(random, "varchar(15)");
  }

  @Test
  public void testDecimal() throws Exception {
    Random random = new Random(9300);

    doIfTests(random, "decimal(38,18)");
    doIfTests(random, "decimal(38,0)");
    doIfTests(random, "decimal(20,8)");
    doIfTests(random, "decimal(10,4)");
  }

  public enum CastStmtTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doIfTests(Random random, String typeName)
      throws Exception {
    doIfTests(random, typeName, DataTypePhysicalVariation.NONE);
  }

  private void doIfTests(Random random, String typeName,
      DataTypePhysicalVariation dataTypePhysicalVariation)
          throws Exception {

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();

    for (PrimitiveCategory targetPrimitiveCategory : PrimitiveCategory.values()) {

      if (targetPrimitiveCategory == PrimitiveCategory.INTERVAL_YEAR_MONTH ||
          targetPrimitiveCategory == PrimitiveCategory.INTERVAL_DAY_TIME) {
        if (primitiveCategory != PrimitiveCategory.STRING) {
          continue;
        }
      }

      if (targetPrimitiveCategory == PrimitiveCategory.VOID ||
          targetPrimitiveCategory == PrimitiveCategory.TIMESTAMPLOCALTZ ||
          targetPrimitiveCategory == PrimitiveCategory.UNKNOWN) {
        continue;
      }

      // DATE conversions NOT supported by integers, floating point, and GenericUDFDecimal.
      if (primitiveCategory == PrimitiveCategory.DATE) {
        if (targetPrimitiveCategory == PrimitiveCategory.BYTE ||
            targetPrimitiveCategory == PrimitiveCategory.SHORT ||
            targetPrimitiveCategory == PrimitiveCategory.INT ||
            targetPrimitiveCategory == PrimitiveCategory.LONG ||
            targetPrimitiveCategory == PrimitiveCategory.FLOAT ||
            targetPrimitiveCategory == PrimitiveCategory.DOUBLE ||
            targetPrimitiveCategory == PrimitiveCategory.DECIMAL) {
          continue;
        }
      }

      if (primitiveCategory == targetPrimitiveCategory) {
        if (primitiveCategory != PrimitiveCategory.CHAR &&
            primitiveCategory != PrimitiveCategory.VARCHAR &&
            primitiveCategory != PrimitiveCategory.DECIMAL) {
          continue;
        }
      }

      doIfTestOneCast(random, typeName, dataTypePhysicalVariation, targetPrimitiveCategory);
    }
  }

  private boolean needsValidDataTypeData(TypeInfo typeInfo) {
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    if (primitiveCategory == PrimitiveCategory.STRING ||
        primitiveCategory == PrimitiveCategory.CHAR ||
        primitiveCategory == PrimitiveCategory.VARCHAR ||
        primitiveCategory == PrimitiveCategory.BINARY) {
      return false;
    }
    return true;
  }

  private void doIfTestOneCast(Random random, String typeName,
      DataTypePhysicalVariation dataTypePhysicalVariation,
      PrimitiveCategory targetPrimitiveCategory)
          throws Exception {

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
    PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();

    boolean isDecimal64 = (dataTypePhysicalVariation == DataTypePhysicalVariation.DECIMAL_64);
    final int decimal64Scale =
        (isDecimal64 ? ((DecimalTypeInfo) typeInfo).getScale() : 0);

    //----------------------------------------------------------------------------------------------

    String targetTypeName;
    if (targetPrimitiveCategory == PrimitiveCategory.BYTE) {
      targetTypeName = "tinyint";
    } else if (targetPrimitiveCategory == PrimitiveCategory.SHORT) {
      targetTypeName = "smallint";
    } else if (targetPrimitiveCategory == PrimitiveCategory.LONG) {
      targetTypeName = "bigint";
    } else {
      targetTypeName = targetPrimitiveCategory.name().toLowerCase();
    }
    targetTypeName = VectorRandomRowSource.getDecoratedTypeName(random, targetTypeName);
    TypeInfo targetTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(targetTypeName);

    //----------------------------------------------------------------------------------------------

    GenerationSpec generationSpec;
    if (needsValidDataTypeData(targetTypeInfo) &&
        (primitiveCategory == PrimitiveCategory.STRING ||
         primitiveCategory == PrimitiveCategory.CHAR ||
         primitiveCategory == PrimitiveCategory.VARCHAR)) {
      generationSpec = GenerationSpec.createStringFamilyOtherTypeValue(typeInfo, targetTypeInfo);
    } else {
      generationSpec = GenerationSpec.createSameType(typeInfo);
    }

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    generationSpecList.add(generationSpec);
    explicitDataTypePhysicalVariationList.add(dataTypePhysicalVariation);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(typeInfo, "col1", "table", false);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[CastStmtTestMode.count][];
    for (int i = 0; i < CastStmtTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      CastStmtTestMode ifStmtTestMode = CastStmtTestMode.values()[i];
      switch (ifStmtTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              typeInfo,
              targetTypeInfo,
              columns,
              children,
              randomRows,
              rowSource.rowStructObjectInspector(),
              resultObjects)) {
          return;
        }
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              typeInfo,
              targetTypeInfo,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              ifStmtTestMode,
              batchSource,
              resultObjects)) {
          return;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + ifStmtTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < CastStmtTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " targetTypeName " + targetTypeName +
                " " + CastStmtTestMode.values()[v] +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (isDecimal64 && expectedResult instanceof LongWritable) {

            HiveDecimalWritable expectedHiveDecimalWritable = new HiveDecimalWritable(0);
            expectedHiveDecimalWritable.deserialize64(
                ((LongWritable) expectedResult).get(), decimal64Scale);
            expectedResult = expectedHiveDecimalWritable;
          }

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " targetTypeName " + targetTypeName +
                " " + CastStmtTestMode.values()[v] +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]));
          }
        }
      }
    }
  }

  private boolean doRowCastTest(TypeInfo typeInfo, TypeInfo targetTypeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ObjectInspector rowInspector, Object[] resultObjects)
          throws Exception {

    GenericUDF udf;
    try {
      udf = VectorizationContext.getGenericUDFForCast(targetTypeInfo);
    } catch (HiveException e) {
      return false;
    }

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(targetTypeInfo, udf, children);

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo +
        " castStmtTestMode ROW_MODE" +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    try {
        evaluator.initialize(rowInspector);
    } catch (HiveException e) {
      return false;
    }

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(targetTypeInfo);

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[i] = copyResult;
    }

    return true;
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow, Object[] resultObjects) {

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);

      try {
      resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);
      } catch (Exception e) {
        System.out.println("here");
      }

      // UNDONE: Need to copy the object.
      resultObjects[rowIndex++] = scrqtchRow[0];
    }
  }

  private boolean doVectorCastTest(TypeInfo typeInfo, TypeInfo targetTypeInfo,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      CastStmtTestMode castStmtTestMode,
      VectorRandomBatchSource batchSource,
      Object[] resultObjects)
          throws Exception {

    GenericUDF udf;
    try {
      udf = VectorizationContext.getGenericUDFForCast(targetTypeInfo);
    } catch (HiveException e) {
      return false;
    }

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(targetTypeInfo, udf, children);

    HiveConf hiveConf = new HiveConf();
    if (castStmtTestMode == CastStmtTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);
    vectorExpression.transientInit(hiveConf);

    if (castStmtTestMode == CastStmtTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " castStmtTestMode " + castStmtTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo +
        " castStmtTestMode " + castStmtTestMode +
        " vectorExpression " + vectorExpression.toString());
    */

    VectorRandomRowSource rowSource = batchSource.getRowSource();
    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            vectorizationContext.getScratchColumnTypeNames(),
            vectorizationContext.getScratchDataTypePhysicalVariations());

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();

    resultVectorExtractRow.init(
        new TypeInfo[] { targetTypeInfo }, new int[] { vectorExpression.getOutputColumnNum() });
    Object[] scrqtchRow = new Object[1];

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow, resultObjects);
      rowIndex += batch.size;
    }

    return true;
  }
}
