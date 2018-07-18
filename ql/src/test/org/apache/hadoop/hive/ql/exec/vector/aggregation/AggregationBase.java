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

package org.apache.hadoop.hive.ql.exec.vector.aggregation;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import junit.framework.Assert;

public class AggregationBase {

  public enum AggregationTestMode {
    ROW_MODE,
    VECTOR_EXPRESSION;

    static final int count = values().length;
  }

  public static GenericUDAFEvaluator getEvaluator(String aggregationFunctionName,
      TypeInfo typeInfo)
      throws SemanticException {

    GenericUDAFResolver resolver =
        FunctionRegistry.getGenericUDAFResolver(aggregationFunctionName);
    TypeInfo[] parameters = new TypeInfo[] { typeInfo };
    GenericUDAFEvaluator evaluator = resolver.getEvaluator(parameters);
    return evaluator;
  }

  protected static boolean doRowTest(TypeInfo typeInfo,
      GenericUDAFEvaluator evaluator, TypeInfo outputTypeInfo,
      GenericUDAFEvaluator.Mode udafEvaluatorMode, int maxKeyCount,
      List<String> columns, List<ExprNodeDesc> children,
      Object[][] randomRows, ObjectInspector rowInspector,
      Object[] results)
          throws Exception {

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " aggregationTestMode ROW_MODE" +
        " outputTypeInfo " + outputTypeInfo.toString());
    */

    // Last entry is for a NULL key.
    AggregationBuffer[] aggregationBuffers = new AggregationBuffer[maxKeyCount + 1];

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(outputTypeInfo);

    Object[] parameterArray = new Object[1];
    final int rowCount = randomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = randomRows[i];
      ShortWritable shortWritable = (ShortWritable) row[0];

      final int key;
      if (shortWritable == null) {
        key = maxKeyCount;
      } else {
        key = shortWritable.get();
      }
      AggregationBuffer aggregationBuffer = aggregationBuffers[key];
      if (aggregationBuffer == null) {
        aggregationBuffer = evaluator.getNewAggregationBuffer();
        aggregationBuffers[key] = aggregationBuffer;
      }
      parameterArray[0] = row[1];
      evaluator.aggregate(aggregationBuffer, parameterArray);
    }

    final boolean isPrimitive = (outputTypeInfo instanceof PrimitiveTypeInfo);
    final boolean isPartial =
        (udafEvaluatorMode == GenericUDAFEvaluator.Mode.PARTIAL1 ||
         udafEvaluatorMode == GenericUDAFEvaluator.Mode.PARTIAL2);

    for (short key = 0; key < maxKeyCount + 1; key++) {
      AggregationBuffer aggregationBuffer = aggregationBuffers[key];
      if (aggregationBuffer != null) {
        final Object result;
        if (isPartial) {
          result = evaluator.terminatePartial(aggregationBuffer);
        } else {
          result = evaluator.terminate(aggregationBuffer);
        }
        Object copyResult;
        if (result == null) {
          copyResult = null;
        } else if (isPrimitive) {
          copyResult =
              VectorRandomRowSource.getWritablePrimitiveObject(
                  (PrimitiveTypeInfo) outputTypeInfo, objectInspector, result);
        } else {
          copyResult =
              ObjectInspectorUtils.copyToStandardObject(
                  result, objectInspector, ObjectInspectorCopyOption.WRITABLE);
        }
        results[key] = copyResult;
      }
    }

    return true;
  }

  private static void extractResultObjects(VectorizedRowBatch outputBatch, short[] keys,
      VectorExtractRow resultVectorExtractRow, TypeInfo outputTypeInfo, Object[] scrqtchRow,
      Object[] results) {

    final boolean isPrimitive = (outputTypeInfo instanceof PrimitiveTypeInfo);
    ObjectInspector objectInspector;
    if (isPrimitive) {
      objectInspector = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(outputTypeInfo);
    } else {
      objectInspector = null;
    }

    for (int batchIndex = 0; batchIndex < outputBatch.size; batchIndex++) {
      resultVectorExtractRow.extractRow(outputBatch, batchIndex, scrqtchRow);
      if (isPrimitive) {
        Object copyResult =
            ObjectInspectorUtils.copyToStandardObject(
                scrqtchRow[0], objectInspector, ObjectInspectorCopyOption.WRITABLE);
        results[keys[batchIndex]] = copyResult;
      } else {
        results[keys[batchIndex]] = scrqtchRow[0];
      }
    }
  }

  protected static boolean doVectorTest(String aggregationName, TypeInfo typeInfo,
      GenericUDAFEvaluator evaluator, TypeInfo outputTypeInfo,
      GenericUDAFEvaluator.Mode udafEvaluatorMode, int maxKeyCount,
      List<String> columns, String[] columnNames,
      TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      List<ExprNodeDesc> parameterList,
      VectorRandomBatchSource batchSource,
      Object[] results)
          throws Exception {

    HiveConf hiveConf = new HiveConf();

    VectorizationContext vectorizationContext =
        new VectorizationContext(
            "name",
            columns,
            Arrays.asList(typeInfos),
            Arrays.asList(dataTypePhysicalVariations),
            hiveConf);

    ImmutablePair<VectorAggregationDesc,String> pair =
        Vectorizer.getVectorAggregationDesc(
            aggregationName,
            parameterList,
            evaluator,
            outputTypeInfo,
            udafEvaluatorMode,
            vectorizationContext);
    VectorAggregationDesc vecAggrDesc = pair.left;
    if (vecAggrDesc == null) {
      Assert.fail(
          "No vector aggregation expression found for aggregationName " + aggregationName +
          " udafEvaluatorMode " + udafEvaluatorMode +
          " parameterList " + parameterList +
          " outputTypeInfo " + outputTypeInfo);
    }

    Class<? extends VectorAggregateExpression> vecAggrClass = vecAggrDesc.getVecAggrClass();

    Constructor<? extends VectorAggregateExpression> ctor = null;
    try {
      ctor = vecAggrClass.getConstructor(VectorAggregationDesc.class);
    } catch (Exception e) {
      throw new HiveException("Constructor " + vecAggrClass.getSimpleName() +
          "(VectorAggregationDesc) not available");
    }
    VectorAggregateExpression vecAggrExpr = null;
    try {
      vecAggrExpr = ctor.newInstance(vecAggrDesc);
    } catch (Exception e) {

       throw new HiveException("Failed to create " + vecAggrClass.getSimpleName() +
           "(VectorAggregationDesc) object ", e);
    }
    VectorExpression.doTransientInit(vecAggrExpr.getInputExpression());

    /*
    System.out.println(
        "*DEBUG* typeInfo " + typeInfo.toString() +
        " aggregationTestMode VECTOR_MODE" +
        " vecAggrExpr " + vecAggrExpr.getClass().getSimpleName());
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

    // Last entry is for a NULL key.
    VectorAggregationBufferRow[] vectorAggregationBufferRows =
        new VectorAggregationBufferRow[maxKeyCount + 1];

    VectorAggregationBufferRow[] batchBufferRows;

    batchSource.resetBatchIteration();
    int rowIndex = 0;
    while (true) {
      if (!batchSource.fillNextBatch(batch)) {
        break;
      }
      LongColumnVector keyLongColVector = (LongColumnVector) batch.cols[0];

      batchBufferRows =
          new VectorAggregationBufferRow[VectorizedRowBatch.DEFAULT_SIZE];

      final int size = batch.size;
      boolean selectedInUse = batch.selectedInUse;
      int[] selected = batch.selected;
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = (selectedInUse ? selected[logical] : logical);
        final int keyAdjustedBatchIndex;
        if (keyLongColVector.isRepeating) {
          keyAdjustedBatchIndex = 0;
        } else {
          keyAdjustedBatchIndex = batchIndex;
        }
        final short key;
        if (keyLongColVector.noNulls || !keyLongColVector.isNull[keyAdjustedBatchIndex]) {
          key = (short) keyLongColVector.vector[keyAdjustedBatchIndex];
        } else {
          key = (short) maxKeyCount;
        }

        VectorAggregationBufferRow bufferRow = vectorAggregationBufferRows[key];
        if (bufferRow == null) {
          VectorAggregateExpression.AggregationBuffer aggregationBuffer =
              vecAggrExpr.getNewAggregationBuffer();
          aggregationBuffer.reset();
          VectorAggregateExpression.AggregationBuffer[] aggregationBuffers =
              new VectorAggregateExpression.AggregationBuffer[] { aggregationBuffer };
          bufferRow = new VectorAggregationBufferRow(aggregationBuffers);
          vectorAggregationBufferRows[key] = bufferRow;
        }
        batchBufferRows[logical] = bufferRow;
      }

      vecAggrExpr.aggregateInputSelection(
          batchBufferRows,
          0,
          batch);

      rowIndex += batch.size;
    }

    String[] outputColumnNames = new String[] { "output" };

    TypeInfo[] outputTypeInfos = new TypeInfo[] { outputTypeInfo };
    VectorizedRowBatchCtx outputBatchContext =
        new VectorizedRowBatchCtx(
            outputColumnNames,
            outputTypeInfos,
            null,
            /* dataColumnNums */ null,
            /* partitionColumnCount */ 0,
            /* virtualColumnCount */ 0,
            /* neededVirtualColumns */ null,
            new String[0],
            new DataTypePhysicalVariation[0]);

    VectorizedRowBatch outputBatch = outputBatchContext.createVectorizedRowBatch();

    short[] keys = new short[VectorizedRowBatch.DEFAULT_SIZE];

    VectorExtractRow resultVectorExtractRow = new VectorExtractRow();
    resultVectorExtractRow.init(
        new TypeInfo[] { outputTypeInfo }, new int[] { 0 });
    Object[] scrqtchRow = new Object[1];

    for (short key = 0; key < maxKeyCount + 1; key++) {
      VectorAggregationBufferRow vectorAggregationBufferRow = vectorAggregationBufferRows[key];
      if (vectorAggregationBufferRow != null) {
        if (outputBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
          extractResultObjects(outputBatch, keys, resultVectorExtractRow, outputTypeInfo,
              scrqtchRow, results);
          outputBatch.reset();
        }
        keys[outputBatch.size] = key;
        VectorAggregateExpression.AggregationBuffer aggregationBuffer =
            vectorAggregationBufferRow.getAggregationBuffer(0);
        vecAggrExpr.assignRowColumn(outputBatch, outputBatch.size++, 0, aggregationBuffer);
      }
    }
    if (outputBatch.size > 0) {
      extractResultObjects(outputBatch, keys, resultVectorExtractRow, outputTypeInfo,
          scrqtchRow, results);
    }

    return true;
  }

  private boolean compareObjects(Object object1, Object object2, TypeInfo typeInfo,
      ObjectInspector objectInspector) {
    if (typeInfo instanceof PrimitiveTypeInfo) {
      return
          VectorRandomRowSource.getWritablePrimitiveObject(
              (PrimitiveTypeInfo) typeInfo, objectInspector, object1).equals(
                  VectorRandomRowSource.getWritablePrimitiveObject(
                      (PrimitiveTypeInfo) typeInfo, objectInspector, object2));
    } else {
      return object1.equals(object2);
    }
  }

  protected void executeAggregationTests(String aggregationName, TypeInfo typeInfo,
      GenericUDAFEvaluator evaluator,
      TypeInfo outputTypeInfo, GenericUDAFEvaluator.Mode udafEvaluatorMode,
      int maxKeyCount, List<String> columns, String[] columnNames,
      List<ExprNodeDesc> parameters, Object[][] randomRows,
      VectorRandomRowSource rowSource, VectorRandomBatchSource batchSource,
      Object[] resultsArray)
          throws Exception {

    for (int i = 0; i < AggregationTestMode.count; i++) {

      // Last entry is for a NULL key.
      Object[] results = new Object[maxKeyCount + 1];
      resultsArray[i] = results;

      AggregationTestMode aggregationTestMode = AggregationTestMode.values()[i];
      switch (aggregationTestMode) {
      case ROW_MODE:
        if (!doRowTest(
              typeInfo,
              evaluator,
              outputTypeInfo,
              udafEvaluatorMode,
              maxKeyCount,
              columns,
              parameters,
              randomRows,
              rowSource.rowStructObjectInspector(),
              results)) {
          return;
        }
        break;
      case VECTOR_EXPRESSION:
        if (!doVectorTest(
              aggregationName,
              typeInfo,
              evaluator,
              outputTypeInfo,
              udafEvaluatorMode,
              maxKeyCount,
              columns,
              columnNames,
              rowSource.typeInfos(),
              rowSource.dataTypePhysicalVariations(),
              parameters,
              batchSource,
              results)) {
          return;
        }
        break;
      default:
        throw new RuntimeException(
            "Unexpected Hash Aggregation test mode " + aggregationTestMode);
      }
    }
  }

  protected void verifyAggregationResults(TypeInfo typeInfo, TypeInfo outputTypeInfo,
      int maxKeyCount, GenericUDAFEvaluator.Mode udafEvaluatorMode,
      Object[] resultsArray) {

    // Row-mode is the expected results.
    Object[] expectedResults = (Object[]) resultsArray[0];

    ObjectInspector objectInspector = TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(outputTypeInfo);

    for (int v = 1; v < AggregationTestMode.count; v++) {
      Object[] vectorResults = (Object[]) resultsArray[v];

      for (short key = 0; key < maxKeyCount + 1; key++) {
        Object expectedResult = expectedResults[key];
        Object vectorResult = vectorResults[key];
        if (expectedResult == null || vectorResult == null) {
          if (expectedResult != null || vectorResult != null) {
            Assert.fail(
                "Key " + key +
                " typeName " + typeInfo.getTypeName() +
                " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + AggregationTestMode.values()[v] +
                " result is NULL " + (vectorResult == null ? "YES" : "NO result " + vectorResult.toString()) +
                " does not match row-mode expected result is NULL " +
                (expectedResult == null ? "YES" : "NO result " + expectedResult.toString()) +
                " udafEvaluatorMode " + udafEvaluatorMode);
          }
        } else {
          if (!compareObjects(expectedResult, vectorResult, outputTypeInfo, objectInspector)) {
            Assert.fail(
                "Key " + key +
              " typeName " + typeInfo.getTypeName() +
              " outputTypeName " + outputTypeInfo.getTypeName() +
                " " + AggregationTestMode.values()[v] +
                " result " + vectorResult.toString() +
                " (" + vectorResult.getClass().getSimpleName() + ")" +
                " does not match row-mode expected result " + expectedResult.toString() +
                " (" + expectedResult.getClass().getSimpleName() + ")" +
                " udafEvaluatorMode " + udafEvaluatorMode);
          }
        }
      }
    }
  }
}