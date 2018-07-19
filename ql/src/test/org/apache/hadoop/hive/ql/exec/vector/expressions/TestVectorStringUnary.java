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
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.StringGenerationOption;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TestVectorTimestampExtract.TimestampExtractTestMode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorStringUnary {

  @Test
  public void testString() throws Exception {
    Random random = new Random(83221);

    doTests(random, "string");
  }

  @Test
  public void testChar() throws Exception {
    Random random = new Random(12882);

    doTests(random, "char(10)");
  }

  @Test
  public void testVarchar() throws Exception {
    Random random = new Random(12882);

    doTests(random, "varchar(15)");
  }

  public enum StringUnaryTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doTests(Random random, String typeName)
      throws Exception {

    if (typeName.equals("string")) {

      // These functions only work on the STRING type.
      doTests(random, typeName, "ltrim");
      doTests(random, typeName, "rtrim");
      doTests(random, typeName, "trim");

      doTests(random, typeName, "initcap");

      doTests(random, typeName, "hex");
    }

    doTests(random, typeName, "lower");
    doTests(random, typeName, "upper");

    doTests(random, typeName, "char_length");
    doTests(random, typeName, "length");
    doTests(random, typeName, "octet_length");
  }

  private void doTests(Random random, String typeName, String functionName)
      throws Exception {

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    TypeInfo targetTypeInfo;
    if (functionName.equals("char_length") ||
        functionName.equals("length") ||
        functionName.equals("octet_length")) {
      targetTypeInfo = TypeInfoFactory.intTypeInfo;
    } else {
      targetTypeInfo = typeInfo;
    }

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 0;
    ExprNodeDesc col1Expr;
    StringGenerationOption stringGenerationOption =
        new StringGenerationOption(true, true);
    generationSpecList.add(
        GenerationSpec.createStringFamily(
            typeInfo, stringGenerationOption));
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

    String columnName = "col" + (columnNum++);
    col1Expr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
    columns.add(columnName);

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0, /* allowNull */ true,
        explicitDataTypePhysicalVariationList);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);
 
    //----------------------------------------------------------------------------------------------

    String[] columnNames = columns.toArray(new String[0]);

    String[] outputScratchTypeNames = new String[] { targetTypeInfo.getTypeName() };
    DataTypePhysicalVariation[] outputDataTypePhysicalVariations =
        new DataTypePhysicalVariation[] { DataTypePhysicalVariation.NONE };

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
            outputDataTypePhysicalVariations);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
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
    Object[][] resultObjectsArray = new Object[StringUnaryTestMode.count][];
    for (int i = 0; i < StringUnaryTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      StringUnaryTestMode stringUnaryTestMode = StringUnaryTestMode.values()[i];
      switch (stringUnaryTestMode) {
      case ROW_MODE:
        doRowIfTest(
            typeInfo, targetTypeInfo,
            columns, children, randomRows, rowSource.rowStructObjectInspector(),
            genericUdf, resultObjects);
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        doVectorIfTest(
            typeInfo,
            targetTypeInfo,
            columns,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            children,
            stringUnaryTestMode,
            batchSource,
            batchContext,
            genericUdf,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected STRING Unary test mode " + stringUnaryTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < StringUnaryTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i + " " + StringUnaryTestMode.values()[v] +
                " typeName " + typeName +
                " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                " functionName " + functionName +
                " genericUdf " + genericUdf.getClass().getSimpleName());
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i + " " + StringUnaryTestMode.values()[v] +
                " typeName " + typeName +
                " result \"" + vectorResult.toString() + "\"" +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result \"" + expectedResult.toString() + "\"" +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " functionName " + functionName +
                " genericUdf " + genericUdf.getClass().getSimpleName());
          }
        }
      }
    }
  }

  private void doRowIfTest(TypeInfo typeInfo, TypeInfo targetTypeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ObjectInspector rowInspector,
      GenericUDF genericUdf, Object[] resultObjects) throws Exception {

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(typeInfo, genericUdf, children);
    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprDesc, hiveConf);
    evaluator.initialize(rowInspector);

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
  }

  private void extractResultObjects(VectorizedRowBatch batch, int rowIndex,
      VectorExtractRow resultVectorExtractRow, Object[] scrqtchRow,
      TypeInfo targetTypeInfo, Object[] resultObjects) {

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(targetTypeInfo);

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    for (int logicalIndex = 0; logicalIndex < batch.size; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);

      try {
        resultVectorExtractRow.extractRow(batch, batchIndex, scrqtchRow);
      } catch (Exception e) {
        Assert.fail(e.toString());
      }

      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              scrqtchRow[0], objectInspector, ObjectInspectorCopyOption.WRITABLE);
      resultObjects[rowIndex++] = copyResult;
    }
  }

  private void doVectorIfTest(TypeInfo typeInfo, TypeInfo targetTypeInfo,
      List<String> columns,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      StringUnaryTestMode stringUnaryTestMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      GenericUDF genericUdf, Object[] resultObjects)
          throws Exception {

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(targetTypeInfo, genericUdf, children);

    HiveConf hiveConf = new HiveConf();
    if (stringUnaryTestMode == StringUnaryTestMode.ADAPTOR) {
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

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(new TypeInfo[] { targetTypeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo.toString() +
        " stringUnaryTestMode " + stringUnaryTestMode +
        " vectorExpression " + vectorExpression.getClass().getSimpleName());
    */

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      vectorExpression.evaluate(batch);
      extractResultObjects(batch, rowIndex, resultVectorExtractRow, scrqtchRow,
          targetTypeInfo, resultObjects);
      rowIndex += batch.size;
    }
  }
}
