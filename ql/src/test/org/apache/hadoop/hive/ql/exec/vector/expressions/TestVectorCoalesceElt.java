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
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.SupportedTypes;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFElt;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;

import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorCoalesceElt {

  @Test
  public void testCoalesce() throws Exception {
    Random random = new Random(5371);

    // Grind through a few more index values...
    int iteration = 0;
    for (int i = 0; i < 10; i++) {
      iteration =  doCoalesceElt(random, iteration, /* isCoalesce */ true, false);
    }
  }

  @Test
  public void testElt() throws Exception {
    Random random = new Random(5371);

    // Grind through a few more index values...
    int iteration = 0;
    for (int i = 0; i < 10; i++) {
      iteration = doCoalesceElt(random, iteration, /* isCoalesce */ false, false);
      iteration = doCoalesceElt(random, iteration, /* isCoalesce */ false, true);
    }
  }

  public enum CoalesceEltTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private int doCoalesceElt(Random random, int iteration, boolean isCoalesce,
      boolean isEltIndexConst)
          throws Exception {

    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 2,
        /* constantColumns */ null, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 2,
        /* constantColumns */ null, /* nullConstantColumns */ null, /* allowNulls */ false);

    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        /* constantColumns */ null, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 0 }, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 0 }, /* nullConstantColumns */ new int[] { 0 }, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 1 }, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 1 }, /* nullConstantColumns */ new int[] { 1 }, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 0, 2 }, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 0, 2 }, /* nullConstantColumns */ new int[] { 0 }, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 3,
        new int[] { 0, 2 }, /* nullConstantColumns */ new int[] { 0, 2 }, /* allowNulls */ false);

    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 4,
        /* constantColumns */ null, /* nullConstantColumns */ null, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 4,
        /* constantColumns */ null, /* nullConstantColumns */ null, /* allowNulls */ false);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 4,
        new int[] { 0, 1, 2 }, /* nullConstantColumns */ new int[] { 0, 1, 2 }, /* allowNulls */ true);
    doCoalesceOnRandomDataType(random, iteration++, isCoalesce, isEltIndexConst, /* columnCount */ 4,
        new int[] { 0, 1, 2 }, /* nullConstantColumns */ new int[] { 0, 1, 2 }, /* allowNulls */ false);
    return iteration;
  }

  private boolean contains(int[] columns, int column) {
    if (columns == null) {
      return false;
    }
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == column) {
        return true;
      }
    }
    return false;
  }

  private boolean doCoalesceOnRandomDataType(Random random, int iteration,
      boolean isCoalesce, boolean isEltIndexConst, int columnCount,
      int[] constantColumns, int[] nullConstantColumns, boolean allowNulls)
      throws Exception {

    String typeName;
    if (isCoalesce) {
      typeName =
          VectorRandomRowSource.getRandomTypeName(
              random, SupportedTypes.PRIMITIVES, /* allowedTypeNameSet */ null);
      typeName =
          VectorRandomRowSource.getDecoratedTypeName(
              random, typeName, SupportedTypes.PRIMITIVES, /* allowedTypeNameSet */ null,
              /* depth */ 0, /* maxDepth */ 2);
    } else {
      // ELT only choose between STRINGs.
      typeName = "string";
    }

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    //----------------------------------------------------------------------------------------------

    final TypeInfo intTypeInfo;
    ObjectInspector intObjectInspector;
    if (isCoalesce) {
      intTypeInfo = null;
      intObjectInspector = null;
    } else {
      intTypeInfo = TypeInfoFactory.intTypeInfo;
      intObjectInspector =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
              intTypeInfo);
    }

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            typeInfo);

    //----------------------------------------------------------------------------------------------

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    List<String> columns = new ArrayList<String>();
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();

    int columnNum = 1;
    if (!isCoalesce) {

      List<Object> intValueList = new ArrayList<Object>();
      for (int i = -1; i < columnCount + 2; i++) {
        intValueList.add(
            new IntWritable(i));
      }
      final int intValueListCount = intValueList.size();
      ExprNodeDesc intColExpr;
      if (!isEltIndexConst) {
        generationSpecList.add(
            GenerationSpec.createValueList(intTypeInfo, intValueList));
        explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
        String columnName = "col" + columnNum++;
        columns.add(columnName);
        intColExpr = new ExprNodeColumnDesc(intTypeInfo, columnName, "table", false);
      } else {
        final Object scalarObject;
        if (random.nextInt(10) != 0) {
          scalarObject = intValueList.get(random.nextInt(intValueListCount));
        } else {
          scalarObject = null;
        }
        intColExpr = new ExprNodeConstantDesc(typeInfo, scalarObject);
      }
      children.add(intColExpr);
    }
    for (int c = 0; c < columnCount; c++) {
      ExprNodeDesc colExpr;
      if (!contains(constantColumns, c)) {

        generationSpecList.add(
            GenerationSpec.createSameType(typeInfo));
        explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);
        String columnName = "col" + columnNum++;
        columns.add(columnName);
        colExpr = new ExprNodeColumnDesc(typeInfo, columnName, "table", false);
      } else {
        final Object scalarObject;
        if (!contains(nullConstantColumns, c)) {
          scalarObject =
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo);
        } else {
          scalarObject = null;
        }
        colExpr = new ExprNodeConstantDesc(typeInfo, scalarObject);
      }
      children.add(colExpr);
    }

    VectorRandomRowSource rowSource = new VectorRandomRowSource();

    rowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ allowNulls,  /* isUnicodeOk */ true,
        explicitDataTypePhysicalVariationList);

    String[] columnNames = columns.toArray(new String[0]);

    Object[][] randomRows = rowSource.randomRows(100000);

    VectorRandomBatchSource batchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            rowSource,
            randomRows,
            null);

    final GenericUDF udf =
        (isCoalesce ? new GenericUDFCoalesce() : new GenericUDFElt());

    final int start = isCoalesce ? 0 : 1;
    final int end = start + columnCount;
    ObjectInspector[] argumentOIs =
        new ObjectInspector[end];
    if (!isCoalesce) {
      argumentOIs[0] = intObjectInspector;
    }
    for (int i = start; i < end; i++) {
      argumentOIs[i] = objectInspector;
    }
    final ObjectInspector outputObjectInspector = udf.initialize(argumentOIs);

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(typeInfo, udf, children);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[CoalesceEltTestMode.count][];
    for (int i = 0; i < CoalesceEltTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      CoalesceEltTestMode coalesceEltTestMode = CoalesceEltTestMode.values()[i];
      switch (coalesceEltTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              typeInfo,
              columns,
              children,
              udf, exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              exprDesc.getWritableObjectInspector(),
              resultObjects)) {
          return false;
        }
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              typeInfo,
              iteration,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              udf, exprDesc,
              coalesceEltTestMode,
              batchSource,
              exprDesc.getWritableObjectInspector(),
              outputTypeInfo,
              resultObjects)) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + coalesceEltTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < CoalesceEltTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        CoalesceEltTestMode coalesceEltTestMode = CoalesceEltTestMode.values()[v];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " " + coalesceEltTestMode +
                " iteration " + iteration +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result '" + expectedResult.toString()) + "'" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " " + coalesceEltTestMode +
                " iteration " + iteration +
                " result '" + vectorResult.toString() + "'" +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result '" + expectedResult.toString() + "'" +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        }
      }
    }
    return true;
  }

  private boolean doRowCastTest(TypeInfo typeInfo,
      List<String> columns, List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector,
      ObjectInspector objectInspector,
      Object[] resultObjects)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo +
        " coalesceEltTestMode ROW_MODE" +
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

    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      Object result = evaluator.evaluate(row);
      Object copyResult =
          ObjectInspectorUtils.copyToStandardObject(
              result, objectInspector,
              ObjectInspectorCopyOption.WRITABLE);
      resultObjects[i] = copyResult;
    }

    return true;
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

  private boolean doVectorCastTest(TypeInfo typeInfo, int iteration,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      CoalesceEltTestMode coalesceEltTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (coalesceEltTestMode == CoalesceEltTestMode.ADAPTOR) {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_TEST_VECTOR_ADAPTOR_OVERRIDE, true);
    }

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);
    VectorExpression vectorExpression =
        vectorizationContext.getVectorExpression(exprDesc, VectorExpressionDescriptor.Mode.PROJECTION);
    vectorExpression.transientInit();

    if (coalesceEltTestMode == CoalesceEltTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " coalesceEltTestMode " + coalesceEltTestMode +
          " vectorExpression " + vectorExpression.toString());
    }

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " coalesceEltTestMode " + coalesceEltTestMode +
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
        new TypeInfo[] { outputTypeInfo }, new int[] { vectorExpression.getOutputColumnNum() });
    Object[] scrqtchRow = new Object[1];

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

    return true;
  }
}
