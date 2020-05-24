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
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.SupportedTypes;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNegative;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorStructField {

  @Test
  public void testStructField() throws Exception {
    Random random = new Random(7743);

    for (int i = 0; i < 5; i++) {
      doStructFieldTests(random);
    }
  }

  public enum StructFieldTestMode {
    ROW_MODE,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doStructFieldTests(Random random) throws Exception {
    String structTypeName =
        VectorRandomRowSource.getDecoratedTypeName(
            random, "struct", SupportedTypes.ALL, /* allowedTypeNameSet */ null,
            /* depth */ 0, /* maxDepth */ 2);
    StructTypeInfo structTypeInfo =
        (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(structTypeName);

    List<String> fieldNameList = structTypeInfo.getAllStructFieldNames();
    final int fieldCount = fieldNameList.size();
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      doOneStructFieldTest(random, structTypeInfo, structTypeName, fieldIndex);
    }
  }

  private void doOneStructFieldTest(Random random, StructTypeInfo structTypeInfo,
      String structTypeName, int fieldIndex)
          throws Exception {

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();

    List<String> columns = new ArrayList<String>();
    int columnNum = 1;

    generationSpecList.add(
        GenerationSpec.createSameType(structTypeInfo));
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

    ExprNodeDesc col1Expr;
    String columnName = "col" + (columnNum++);
    col1Expr = new ExprNodeColumnDesc(structTypeInfo, columnName, "table", false);
    columns.add(columnName);

    ObjectInspector structObjectInspector =
        VectorRandomRowSource.getObjectInspector(structTypeInfo);
    List<ObjectInspector> objectInspectorList = new ArrayList<ObjectInspector>();
    objectInspectorList.add(structObjectInspector);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(col1Expr);

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

    List<String> fieldNameList = structTypeInfo.getAllStructFieldNames();
    List<TypeInfo> fieldTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();

    String randomFieldName = fieldNameList.get(fieldIndex);
    TypeInfo outputTypeInfo = fieldTypeInfoList.get(fieldIndex);

    ExprNodeFieldDesc exprNodeFieldDesc =
        new ExprNodeFieldDesc(outputTypeInfo, col1Expr, randomFieldName, /* isList */ false);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[StructFieldTestMode.count][];
    for (int i = 0; i < StructFieldTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      StructFieldTestMode negativeTestMode = StructFieldTestMode.values()[i];
      switch (negativeTestMode) {
      case ROW_MODE:
        doRowStructFieldTest(
            structTypeInfo,
            columns,
            children,
            exprNodeFieldDesc,
            randomRows,
            rowSource.rowStructObjectInspector(),
            outputTypeInfo,
            resultObjects);
        break;
      case VECTOR_EXPRESSION:
        doVectorStructFieldTest(
            structTypeInfo,
            columns,
            columnNames,
            rowSource.typeInfos(),
            rowSource.dataTypePhysicalVariations(),
            children,
            exprNodeFieldDesc,
            negativeTestMode,
            batchSource,
            exprNodeFieldDesc.getWritableObjectInspector(),
            outputTypeInfo,
            resultObjects);
        break;
      default:
        throw new RuntimeException("Unexpected Negative operator test mode " + negativeTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < StructFieldTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " structTypeName " + structTypeName +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + StructFieldTestMode.values()[v] +
               " result is NULL " + (vectorResult == null) +
                " does not match row-mode expected result is NULL " + (expectedResult == null) +
                " row values " + Arrays.toString(randomRows[i]));
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " structTypeName " + structTypeName +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + StructFieldTestMode.values()[v] +
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

  private void doRowStructFieldTest(TypeInfo typeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      ExprNodeFieldDesc exprNodeFieldDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects) throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " negativeTestMode ROW_MODE" +
        " exprDesc " + exprDesc.toString());
    */

    HiveConf hiveConf = new HiveConf();
    ExprNodeEvaluator evaluator =
        ExprNodeEvaluatorFactory.get(exprNodeFieldDesc, hiveConf);
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
        System.out.println("here");
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

  private void doVectorStructFieldTest(TypeInfo typeInfo,
      List<String> columns,
      String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      ExprNodeFieldDesc exprNodeFieldDesc,
      StructFieldTestMode negativeTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression =
        vectorizationContext.getVectorExpression(exprNodeFieldDesc);
    vectorExpression.transientInit(hiveConf);

    if (negativeTestMode == StructFieldTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " negativeTestMode " + negativeTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    String[] outputScratchTypeNames= vectorizationContext.getScratchColumnTypeNames();

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
            null);

    VectorizedRowBatch batch = batchContext.createVectorizedRowBatch();

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(
        new TypeInfo[] { outputTypeInfo }, new int[] { vectorExpression.getOutputColumnNum() });
    Object[] scrqtchRow = new Object[1];

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " negativeTestMode " + negativeTestMode +
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
