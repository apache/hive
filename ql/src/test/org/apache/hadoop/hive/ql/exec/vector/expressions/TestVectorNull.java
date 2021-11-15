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
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.SupportedTypes;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IdentityExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.TestVectorArithmetic.ColumnScalarMode;
import org.apache.hadoop.hive.ql.exec.vector.udf.VectorUDFAdaptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;

import org.junit.Assert;

import org.junit.Ignore;
import org.junit.Test;

public class TestVectorNull {

  @Test
  public void testIsNull() throws Exception {
    Random random = new Random(5371);

    doNull(random, "isnull");
  }

  @Test
  public void testIsNotNull() throws Exception {
    Random random = new Random(2772);

    doNull(random, "isnotnull");
  }

  @Test
  public void testNot() throws Exception {
    Random random = new Random(2772);

    doNull(random, "not");
  }

  public enum NullTestMode {
    ROW_MODE,
    ADAPTOR,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  private void doNull(Random random, String functionName)
      throws Exception {

    // Several different random types...
    doIsNullOnRandomDataType(random, functionName, true);
    doIsNullOnRandomDataType(random, functionName, true);
    doIsNullOnRandomDataType(random, functionName, true);

    doIsNullOnRandomDataType(random, functionName, false);
    doIsNullOnRandomDataType(random, functionName, false);
    doIsNullOnRandomDataType(random, functionName, false);
  }

  private boolean doIsNullOnRandomDataType(Random random, String functionName, boolean isFilter)
      throws Exception {

    String typeName;
    if (functionName.equals("not")) {
      typeName = "boolean";
    } else {
      typeName =
          VectorRandomRowSource.getRandomTypeName(
              random, SupportedTypes.ALL, /* allowedTypeNameSet */ null);
      typeName =
          VectorRandomRowSource.getDecoratedTypeName(
              random, typeName, SupportedTypes.ALL, /* allowedTypeNameSet */ null,
              /* depth */ 0, /* maxDepth */ 2);
    }

    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);

    //----------------------------------------------------------------------------------------------

    ObjectInspector objectInspector =
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
            typeInfo);

    //----------------------------------------------------------------------------------------------

    GenerationSpec generationSpec = GenerationSpec.createSameType(typeInfo);

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    List<DataTypePhysicalVariation> explicitDataTypePhysicalVariationList =
        new ArrayList<DataTypePhysicalVariation>();
    generationSpecList.add(generationSpec);
    explicitDataTypePhysicalVariationList.add(DataTypePhysicalVariation.NONE);

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

    final GenericUDF udf;
    final ObjectInspector outputObjectInspector;
    switch (functionName) {
    case "isnull":
      udf = new GenericUDFOPNull();
      break;
    case "isnotnull":
      udf = new GenericUDFOPNotNull();
      break;
    case "not":
      udf = new GenericUDFOPNot();
      break;
    default:
      throw new RuntimeException("Unexpected function name " + functionName);
    }

    ObjectInspector[] argumentOIs = new ObjectInspector[] { objectInspector };
    outputObjectInspector = udf.initialize(argumentOIs);

    TypeInfo outputTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(outputObjectInspector);

    ExprNodeGenericFuncDesc exprDesc =
        new ExprNodeGenericFuncDesc(
            TypeInfoFactory.booleanTypeInfo, udf, children);

    final int rowCount = randomRows.length;
    Object[][] resultObjectsArray = new Object[NullTestMode.count][];
    for (int i = 0; i < NullTestMode.count; i++) {

      Object[] resultObjects = new Object[rowCount];
      resultObjectsArray[i] = resultObjects;

      NullTestMode nullTestMode = NullTestMode.values()[i];
      switch (nullTestMode) {
      case ROW_MODE:
        if (!doRowCastTest(
              typeInfo,
              isFilter,
              columns,
              children,
              udf, exprDesc,
              randomRows,
              rowSource.rowStructObjectInspector(),
              resultObjects)) {
          return false;
        }
        break;
      case ADAPTOR:
      case VECTOR_EXPRESSION:
        if (!doVectorCastTest(
              typeInfo,
              isFilter,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              children,
              udf, exprDesc,
              nullTestMode,
              batchSource,
              exprDesc.getWritableObjectInspector(),
              outputTypeInfo,
              resultObjects)) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unexpected IF statement test mode " + nullTestMode);
      }
    }

    for (int i = 0; i < rowCount; i++) {
      // Row-mode is the expected value.
      Object expectedResult = resultObjectsArray[0][i];

      for (int v = 1; v < NullTestMode.count; v++) {
        Object vectorResult = resultObjectsArray[v][i];
        NullTestMode nullTestMode = NullTestMode.values()[v];
        if (isFilter &&
            expectedResult == null &&
            vectorResult != null) {
          // This is OK.
          boolean vectorBoolean = ((BooleanWritable) vectorResult).get();
          if (vectorBoolean) {
            Assert.fail(
                "Row " + i +
                " typeName " + typeName +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " isFilter " + isFilter +
                " " + nullTestMode +
                " result is NOT NULL and true" +
                " does not match row-mode expected result is NULL which means false here" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        } else if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " isFilter " + isFilter +
                " " + nullTestMode +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        } else {

          if (!expectedResult.equals(vectorResult)) {
            Assert.fail(
                "Row " + i +
                " sourceTypeName " + typeName +
                " isFilter " + isFilter +
                " " + nullTestMode +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " row values " + Arrays.toString(randomRows[i]) +
                " exprDesc " + exprDesc.toString());
          }
        }
      }
    }
    return true;
  }

  private boolean doRowCastTest(TypeInfo typeInfo, boolean isFilter,
      List<String> columns, List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      Object[][] randomRows,
      ObjectInspector rowInspector, Object[] resultObjects)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " targetTypeInfo " + targetTypeInfo +
        " nullTestMode ROW_MODE" +
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
              result, PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
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

  private boolean doVectorCastTest(TypeInfo typeInfo, boolean isFilter,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> children,
      GenericUDF udf, ExprNodeGenericFuncDesc exprDesc,
      NullTestMode nullTestMode,
      VectorRandomBatchSource batchSource,
      ObjectInspector objectInspector,
      TypeInfo outputTypeInfo, Object[] resultObjects)
          throws Exception {

    HiveConf hiveConf = new HiveConf();
    if (nullTestMode == NullTestMode.ADAPTOR) {
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
        vectorizationContext.getVectorExpression(exprDesc,
            (isFilter ?
                VectorExpressionDescriptor.Mode.FILTER :
                VectorExpressionDescriptor.Mode.PROJECTION));
    vectorExpression.transientInit(hiveConf);

    if (nullTestMode == NullTestMode.VECTOR_EXPRESSION &&
        vectorExpression instanceof VectorUDFAdaptor) {
      System.out.println(
          "*NO NATIVE VECTOR EXPRESSION* typeInfo " + typeInfo.toString() +
          " nullTestMode " + nullTestMode +
          " isFilter " + isFilter +
          " vectorExpression " + vectorExpression.toString());
    }

    // System.out.println("*VECTOR EXPRESSION* " + vectorExpression.getClass().getSimpleName());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " nullTestMode " + nullTestMode +
        " isFilter " + isFilter +
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

    VectorExtractRow resultVectorExtractRow = null;
    Object[] scrqtchRow = null;
    if (!isFilter) {
      resultVectorExtractRow = new VectorExtractRow();
      final int outputColumnNum = vectorExpression.getOutputColumnNum();
      resultVectorExtractRow.init(
          new TypeInfo[] { outputTypeInfo }, new int[] { outputColumnNum });
      scrqtchRow = new Object[1];
    }

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

    return true;
  }
}
