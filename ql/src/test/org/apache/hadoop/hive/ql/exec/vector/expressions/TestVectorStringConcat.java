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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateAdd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateSub;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
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
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorStringConcat {

  @Test
  public void testString() throws Exception {
    Random random = new Random(12882);

    doStringConcatTests(random, "string", "string");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doStringConcatTests(random, "char(20)", "char(10)");
    doStringConcatTests(random, "char(20)", "string");
    doStringConcatTests(random, "char(20)", "varchar(10)");
    doStringConcatTests(random, "string", "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doStringConcatTests(random, "varchar(20)", "varchar(10)");
    doStringConcatTests(random, "varchar(20)", "string");
    doStringConcatTests(random, "varchar(20)", "char(10)");
    doStringConcatTests(random, "string", "varchar(10)");
  }

  public enum StringConcatTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public enum ColumnScalarMode {
    COLUMN_COLUMN,
    COLUMN_SCALAR,
    SCALAR_COLUMN;

    static final int count = values().length;
  }

  private void doStringConcatTests(Random random, String stringTypeName1, String stringTypeName2)
          throws Exception {
    for (ColumnScalarMode columnScalarMode : ColumnScalarMode.values()) {
      doStringConcatTestsWithDiffColumnScalar(
          random, stringTypeName1, stringTypeName2, columnScalarMode);
    }
  }

  private void doStringConcatTestsWithDiffColumnScalar(Random random,
      String stringTypeName1, String stringTypeName2, ColumnScalarMode columnScalarMode)
          throws Exception {

    TypeInfo stringTypeInfo1 =
        TypeInfoUtils.getTypeInfoFromTypeString(stringTypeName1);
    PrimitiveCategory stringPrimitiveCategory1 =
        ((PrimitiveTypeInfo) stringTypeInfo1).getPrimitiveCategory();

    TypeInfo stringTypeInfo2 =
        TypeInfoUtils.getTypeInfoFromTypeString(stringTypeName2);
    PrimitiveCategory stringPrimitiveCategory2 =
        ((PrimitiveTypeInfo) stringTypeInfo2).getPrimitiveCategory();

    String functionName = "concat";

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;
    ExprNodeDesc col1Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.COLUMN_SCALAR) {
      generationSpecList.add(
          GenerationSpec.createSameType(stringTypeInfo1));

      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col1Expr = new ExprNodeColumnDesc(stringTypeInfo1, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar1Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) stringTypeInfo1);
      col1Expr = new ExprNodeConstantDesc(stringTypeInfo1, scalar1Object);
    }
    ExprNodeDesc col2Expr;
    if (columnScalarMode == ColumnScalarMode.COLUMN_COLUMN ||
        columnScalarMode == ColumnScalarMode.SCALAR_COLUMN) {
      generationSpecList.add(
          GenerationSpec.createSameType(stringTypeInfo2));
      explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

      String columnName = "col" + (columnNum++);
      col2Expr = new ExprNodeColumnDesc(stringTypeInfo2, columnName, "table", false);
      columns.add(columnName);
    } else {
      Object scalar2Object =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) stringTypeInfo2);
      col2Expr = new ExprNodeConstantDesc(stringTypeInfo2, scalar2Object);
    }

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
    children.add(col2Expr);

    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    String[] outputScratchTypeNames = new String[] { "string" };

    VectorizedRowBatchCtx batchContext =
        new VectorizedRowBatchCtx(
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            outputScratchTypeNames,
            null);

    GenericUDF genericUdf;
    FunctionInfo funcInfo = null;
    try {
      funcInfo = FunctionRegistry.getFunctionInfo(functionName);
    } catch (SemanticException e) {
      Assert.fail("Failed to load " + functionName + " " + e);
    }
    genericUdf = funcInfo.getGenericUDF();

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[StringConcatTestMode.count][];
    for (int i = 0; i < StringConcatTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

       StringConcatTestMode stringConcatTestMode = StringConcatTestMode.values()[i];
      switch (stringConcatTestMode) {
      case ROW_MODE:
        doRowStringConcatTest(
            stringTypeInfo1,
            stringTypeInfo2,
            columns,
            children,
            randomRows,
            columnScalarMode,
            rowSource.rowStructObjectInspector(),
            genericUdf,
            resultObjects);
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        doVectorStringConcatTest(
            stringTypeInfo1,
            stringTypeInfo2,
            columns,
            rowSource.typeInfos(),
            children,
            stringConcatTestMode,
            columnScalarMode,
            batchSource,
            batchContext,
            rowSource.rowStructObjectInspector(),
            genericUdf,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + stringConcatTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < StringConcatTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + StringConcatTestMode.values()[v] +
                " " + columnScalarMode +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + StringConcatTestMode.values()[v] +
                " " + columnScalarMode +
                " result \"" + vectorResult.toString() + "\"" +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result \"" + expectedResult.toString() + "\"" +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]));
          }
        }
      }
    }
  }

  private void doRowStringConcatTest(TypeInfo stringTypeInfo, TypeInfo integerTypeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ColumnScalarMode columnScalarMode,
      ObjectInspector rowInspector,
      GenericUDF genericUdf, Object[] resultObjects) throws Exception {

    /*
    System.out.println(
        "*DEBUG* stringTypeInfo " + stringTypeInfo.toString() +
        " integerTypeInfo " + integerTypeInfo +
        " stringConcatTestMode ROW_MODE" +
        " columnScalarMode " + columnScalarMode +
        " genericUdf " + genericUdf.toString());
    */

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, genericUdf, children);

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector = evaluator.getOutputOI();
 
    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
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

  private void doVectorStringConcatTest(TypeInfo stringTypeInfo1, TypeInfo stringTypeInfo2,
      List<String> columns,
      TypeInfo[] typeInfos,
      List<ExprNodeDesc> children,
      StringConcatTestMode stringConcatTestMode, ColumnScalarMode columnScalarMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      ObjectInspector rowInspector,
      GenericUDF genericUdf, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (stringConcatTestMode == StringConcatTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    DataTypePhysicalVariation[] dataTypePhysicalVariations = new DataTypePhysicalVariation[2];
    Arrays.fill(dataTypePhysicalVariations, DataTypePhysicalVariation.NONE);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo, genericUdf, children);

    //---------------------------------------
    // Just so we can get the output type...

    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

    ObjectInspector objectInspector = evaluator.getOutputOI();
    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector);

    /*
     * Again with correct output type...
     */
    exprDesc =
        new ExprNodeGenericFuncDesc(outputTypeInfo, genericUdf, children);
    //---------------------------------------

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression = vectorizationContext.getVectorExpression(exprDesc);
    vectorExpression.transientInit();

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(
        new TypeInfo[] { outputTypeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    /*
    System.out.println(
        "*DEBUG* stringTypeInfo1 " + stringTypeInfo1.toString() +
        " stringTypeInfo2 " + stringTypeInfo2.toString() +
        " stringConcatTestMode " + stringConcatTestMode +
        " columnScalarMode " + columnScalarMode +
        " vectorExpression " + vectorExpression.toString());
    */

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
          objectInspector, resultObjects);
      rowIndex += batch.size;
    }
  }
}
