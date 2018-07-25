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
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
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
import org.apache.hadoop.io.IntWritable;

import junit.framework.Assert;

import org.junit.Test;

public class TestVectorSubStr {

  @Test
  public void testString() throws Exception {
    Random random = new Random(83221);

    doTests(random);
  }

  public enum SubStrTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doTests(Random random)
      throws Exception {

    for (int i = 0; i < 50; i++) {
      doTests(random, false);
      doTests(random, true);
    }
  }

  private void doTests(Random random, boolean useLength)
      throws Exception {

    String typeName = "string";
    TypeInfo typeInfo = TypeInfoFactory.stringTypeInfo;
    TypeInfo targetTypeInfo = typeInfo;
    String functionName = "substr";

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

    final int position = 10 - random.nextInt(21);
    Object scalar2Object =
        Integer.valueOf(position);
    ExprNodeDesc col2Expr = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, scalar2Object);
    children.add(col2Expr);

    if (useLength) {

      Object scalar3Object = random.nextInt(12);
      ExprNodeDesc col3Expr = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, scalar3Object);
      children.add(col3Expr);
    }

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
    Object[][] resultObjectsArray = new Object[SubStrTestMode.count][];
    for (int i = 0; i < SubStrTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      SubStrTestMode subStrTestMode = SubStrTestMode.values()[i];
      switch (subStrTestMode) {
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
            subStrTestMode,
            batchSource,
            batchContext,
            genericUdf,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected STRING Unary test mode " + subStrTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < SubStrTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " " + SubStrTestMode.values()[v] +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " " + SubStrTestMode.values()[v] +
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
      SubStrTestMode subStrTestMode,
      VectorRandomBatchSource batchSource, VectorizedRowBatchCtx batchContext,
      GenericUDF genericUdf, Object[] resultObjects)
          throws Exception {

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(targetTypeInfo, genericUdf, children);

    HiveConf hiveConf = new HiveConf();
    if (subStrTestMode == SubStrTestMode.ADAPTOR) {
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

    if (subStrTestMode == SubStrTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " subStrTestMode " + subStrTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(new TypeInfo[] { targetTypeInfo }, new int[] { columns.size() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo.toString() +
        " subStrTestMode " + subStrTestMode +
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
