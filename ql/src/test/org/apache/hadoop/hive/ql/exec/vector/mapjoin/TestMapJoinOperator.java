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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinBytesTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.CountVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowCollectorTestOperatorBase;
import org.apache.hadoop.hive.ql.exec.util.collectoroperator.RowVectorCollectorTestOperator;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjectsMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorBatchDebug;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapJoinOuterFilteredOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerateStream;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestConfig.MapJoinTestImplementation;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastMultiKeyHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VerifyFastRow;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HashCodeUtil;
import org.apache.hive.common.util.ReflectionUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;

public class TestMapJoinOperator {

  /*
   * This test collector operator is for MapJoin row-mode.
   */
  private class TestMultiSetCollectorOperator extends RowCollectorTestOperator {

    private final RowTestObjectsMultiSet testRowMultiSet;

    public TestMultiSetCollectorOperator(
        ObjectInspector[] outputObjectInspectors,
        RowTestObjectsMultiSet testRowMultiSet) {
      super(outputObjectInspectors);
      this.testRowMultiSet = testRowMultiSet;
    }

    public RowTestObjectsMultiSet getTestRowMultiSet() {
      return testRowMultiSet;
    }

    public void nextTestRow(RowTestObjects testRow) {
      testRowMultiSet.add(testRow);
    }

    @Override
    public String getName() {
      return TestMultiSetCollectorOperator.class.getSimpleName();
    }
  }

  private class TestMultiSetVectorCollectorOperator extends RowVectorCollectorTestOperator {

    private final RowTestObjectsMultiSet testRowMultiSet;

    public RowTestObjectsMultiSet getTestRowMultiSet() {
      return testRowMultiSet;
    }

    public TestMultiSetVectorCollectorOperator(TypeInfo[] outputTypeInfos,
        ObjectInspector[] outputObjectInspectors, RowTestObjectsMultiSet testRowMultiSet)
            throws HiveException {
      super(outputTypeInfos, outputObjectInspectors);
      this.testRowMultiSet = testRowMultiSet;
     }

    public void nextTestRow(RowTestObjects testRow) {
      testRowMultiSet.add(testRow);
    }

    @Override
    public String getName() {
      return TestMultiSetVectorCollectorOperator.class.getSimpleName();
    }
  }

  private static class KeyConfig {
    long seed;
    PrimitiveTypeInfo primitiveTypeInfo;
    KeyConfig(long seed, PrimitiveTypeInfo primitiveTypeInfo) {
      this.seed = seed;
      this.primitiveTypeInfo = primitiveTypeInfo;
    }
  }
  private static KeyConfig[] longKeyConfigs = new KeyConfig[] {
    new KeyConfig(234882L, TypeInfoFactory.longTypeInfo),
    new KeyConfig(4600L, TypeInfoFactory.intTypeInfo),
    new KeyConfig(98743L, TypeInfoFactory.shortTypeInfo)};

  @Test
  public void testLong() throws Exception {
    for (KeyConfig longKeyConfig : longKeyConfigs) {
      for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
        if (vectorMapJoinVariation == VectorMapJoinVariation.NONE){
          continue;
        }
        doTestLong(longKeyConfig.seed, longKeyConfig.primitiveTypeInfo, vectorMapJoinVariation);
      }
    }
  }

  public void doTestLong(long seed, TypeInfo numberTypeInfo,
      VectorMapJoinVariation vectorMapJoinVariation) throws Exception {

    int rowCount = 10000;

    HiveConf hiveConf = new HiveConf();

    String[] bigTableColumnNames = new String[] {"number1"};
    TypeInfo[] bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.longTypeInfo};
    int[] bigTableKeyColumnNums = new int[] {0};

    String[] smallTableValueColumnNames = new String[] {"sv1", "sv2"};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo, TypeInfoFactory.stringTypeInfo};

    int[] bigTableRetainColumnNums = new int[] {0};

    int[] smallTableRetainKeyColumnNums = new int[] {};
    int[] smallTableRetainValueColumnNums = new int[] {0, 1};

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    //----------------------------------------------------------------------------------------------

    MapJoinTestDescription testDesc = new MapJoinTestDescription(
        hiveConf, vectorMapJoinVariation,
        bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueColumnNames, smallTableValueTypeInfos,
        bigTableRetainColumnNums,
        smallTableRetainKeyColumnNums, smallTableRetainValueColumnNums,
        smallTableGenerationParameters);

    // Prepare data.  Good for ANY implementation variation.
    MapJoinTestData testData =
        new MapJoinTestData(rowCount, testDesc, seed, seed * 10);

    executeTest(testDesc, testData);
  }

  @Test
  public void testMultiKey() throws Exception {
    long seed = 87543;
    for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
      if (vectorMapJoinVariation == VectorMapJoinVariation.NONE){
        continue;
      }
      doTestMultiKey(seed, vectorMapJoinVariation);
    }
  }

  public void doTestMultiKey(long seed, VectorMapJoinVariation vectorMapJoinVariation) throws Exception {

    int rowCount = 10000;

    HiveConf hiveConf = new HiveConf();

    String[] bigTableColumnNames = new String[] {"b1", "b2", "b3"};
    TypeInfo[] bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.intTypeInfo,
            TypeInfoFactory.longTypeInfo,
            TypeInfoFactory.stringTypeInfo};
    int[] bigTableKeyColumnNums = new int[] {0, 1, 2};

    String[] smallTableValueColumnNames = new String[] {"sv1"};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.stringTypeInfo};

    int[] bigTableRetainColumnNums = new int[] {0, 1, 2};

    int[] smallTableRetainKeyColumnNums = new int[] {};
    int[] smallTableRetainValueColumnNums = new int[] {0};

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    //----------------------------------------------------------------------------------------------

    MapJoinTestDescription testDesc = new MapJoinTestDescription(
        hiveConf, vectorMapJoinVariation,
        bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueColumnNames, smallTableValueTypeInfos,
        bigTableRetainColumnNums,
        smallTableRetainKeyColumnNums, smallTableRetainValueColumnNums,
        smallTableGenerationParameters);

    // Prepare data.  Good for ANY implementation variation.
    MapJoinTestData testData =
        new MapJoinTestData(rowCount, testDesc, seed, seed * 10);

    executeTest(testDesc, testData);
  }

  @Test
  public void testString() throws Exception {
    long seed = 87543;
    for (VectorMapJoinVariation vectorMapJoinVariation : VectorMapJoinVariation.values()) {
      if (vectorMapJoinVariation == VectorMapJoinVariation.NONE){
        continue;
      }
      doTestString(seed, vectorMapJoinVariation);
    }
  }

  public void doTestString(long seed, VectorMapJoinVariation vectorMapJoinVariation) throws Exception {

    int rowCount = 10000;

    HiveConf hiveConf = new HiveConf();

    String[] bigTableColumnNames = new String[] {"b1"};
    TypeInfo[] bigTableTypeInfos =
        new TypeInfo[] {
            TypeInfoFactory.stringTypeInfo};
    int[] bigTableKeyColumnNums = new int[] {0};

    String[] smallTableValueColumnNames = new String[] {"sv1", "sv2"};
    TypeInfo[] smallTableValueTypeInfos =
        new TypeInfo[] {TypeInfoFactory.dateTypeInfo, TypeInfoFactory.timestampTypeInfo};

    int[] bigTableRetainColumnNums = new int[] {0};

    int[] smallTableRetainKeyColumnNums = new int[] {};
    int[] smallTableRetainValueColumnNums = new int[] {0, 1};

    SmallTableGenerationParameters smallTableGenerationParameters =
        new SmallTableGenerationParameters();

    //----------------------------------------------------------------------------------------------

    MapJoinTestDescription testDesc = new MapJoinTestDescription(
        hiveConf, vectorMapJoinVariation,
        bigTableColumnNames, bigTableTypeInfos,
        bigTableKeyColumnNums,
        smallTableValueColumnNames, smallTableValueTypeInfos,
        bigTableRetainColumnNums,
        smallTableRetainKeyColumnNums, smallTableRetainValueColumnNums,
        smallTableGenerationParameters);

    // Prepare data.  Good for ANY implementation variation.
    MapJoinTestData testData =
        new MapJoinTestData(rowCount, testDesc, seed, seed * 10);

    executeTest(testDesc, testData);
  }

  private void addBigTableRetained(MapJoinTestDescription testDesc, Object[] bigTableRowObjects,
      Object[] outputObjects) {
    final int bigTableRetainColumnNumsLength = testDesc.bigTableRetainColumnNums.length;
    for (int o = 0; o < bigTableRetainColumnNumsLength; o++) {
      outputObjects[o] = bigTableRowObjects[testDesc.bigTableRetainColumnNums[o]];
    }
  }

  private void addToOutput(MapJoinTestDescription testDesc, RowTestObjectsMultiSet expectedTestRowMultiSet,
      Object[] outputObjects) {
    for (int c = 0; c < outputObjects.length; c++) {
      PrimitiveObjectInspector primitiveObjInsp = ((PrimitiveObjectInspector) testDesc.outputObjectInspectors[c]);
      Object outputObject = outputObjects[c];
      outputObjects[c] = primitiveObjInsp.copyObject(outputObject);
    }
    expectedTestRowMultiSet.add(new RowTestObjects(outputObjects));
  }

  /*
   * Simulate the join by driving the test big table data by our test small table HashMap and
   * create the expected output as a multi-set of TestRow (i.e. TestRow and occurrence count).
   */
  private RowTestObjectsMultiSet createExpectedTestRowMultiSet(MapJoinTestDescription testDesc,
      MapJoinTestData testData) throws HiveException {

    RowTestObjectsMultiSet expectedTestRowMultiSet = new RowTestObjectsMultiSet();

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(testDesc.bigTableKeyTypeInfos);

    final int bigTableColumnCount = testDesc.bigTableTypeInfos.length;
    Object[] bigTableRowObjects = new Object[bigTableColumnCount];

    final int bigTableKeyColumnCount = testDesc.bigTableKeyTypeInfos.length;
    Object[] bigTableKeyObjects = new Object[bigTableKeyColumnCount];

    VectorBatchGenerateStream bigTableBatchStream = testData.getBigTableBatchStream();
    VectorizedRowBatch batch = testData.getBigTableBatch();
    bigTableBatchStream.reset();
    while (bigTableBatchStream.isNext()) {
      batch.reset();
      bigTableBatchStream.fillNext(batch);

      final int size = testData.bigTableBatch.size;
      for (int r = 0; r < size; r++) {
        vectorExtractRow.extractRow(testData.bigTableBatch, r, bigTableRowObjects);

        // Form key object array
        for (int k = 0; k < bigTableKeyColumnCount; k++) {
          int keyColumnNum = testDesc.bigTableKeyColumnNums[k];
          bigTableKeyObjects[k] = bigTableRowObjects[keyColumnNum];
          bigTableKeyObjects[k] = ((PrimitiveObjectInspector) testDesc.bigTableObjectInspectors[keyColumnNum]).copyObject(bigTableKeyObjects[k]);
        }
        RowTestObjects testKey = new RowTestObjects(bigTableKeyObjects);

        if (testData.smallTableKeyHashMap.containsKey(testKey)) {

          int smallTableKeyIndex = testData.smallTableKeyHashMap.get(testKey);

          switch (testDesc.vectorMapJoinVariation) {
          case INNER:
          case OUTER:
            {
              // One row per value.
              ArrayList<RowTestObjects> valueList = testData.smallTableValues.get(smallTableKeyIndex);
              final int valueCount = valueList.size();
              for (int v = 0; v < valueCount; v++) {
                Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

                addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);

                Object[] valueRow = valueList.get(v).getRow();
                final int bigTableRetainColumnNumsLength = testDesc.bigTableRetainColumnNums.length;
                final int smallTableRetainValueColumnNumsLength = testDesc.smallTableRetainValueColumnNums.length;
                for (int o = 0; o < smallTableRetainValueColumnNumsLength; o++) {
                  outputObjects[bigTableRetainColumnNumsLength + o] = valueRow[testDesc.smallTableRetainValueColumnNums[o]];
                }

                addToOutput(testDesc, expectedTestRowMultiSet, outputObjects);
              }
            }
            break;
          case INNER_BIG_ONLY:
            {
              // Value count rows.
              final int valueCount = testData.smallTableValueCounts.get(smallTableKeyIndex);
              for (int v = 0; v < valueCount; v++) {
                Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

                addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);
                addToOutput(testDesc, expectedTestRowMultiSet, outputObjects);
              }
            }
            break;
          case LEFT_SEMI:
            {
              // One row (existence).
              Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

              addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);
              addToOutput(testDesc, expectedTestRowMultiSet, outputObjects);
            }
            break;
          default:
            throw new RuntimeException("Unknown operator variation " + testDesc.vectorMapJoinVariation);
          }

        } else {

          // No match.

          if (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.OUTER) {

            // We need to add a non-match row with nulls for small table values.

            Object[] outputObjects = new Object[testDesc.outputColumnNames.length];

            addBigTableRetained(testDesc, bigTableRowObjects, outputObjects);

            final int bigTableRetainColumnNumsLength = testDesc.bigTableRetainColumnNums.length;
            final int smallTableRetainValueColumnNumsLength = testDesc.smallTableRetainValueColumnNums.length;
            for (int o = 0; o < smallTableRetainValueColumnNumsLength; o++) {
              outputObjects[bigTableRetainColumnNumsLength + o] = null;
            }

            addToOutput(testDesc, expectedTestRowMultiSet, outputObjects);
          }
        }
      }
    }

    return expectedTestRowMultiSet;
  }

  private void executeTest(MapJoinTestDescription testDesc, MapJoinTestData testData) throws Exception {

    RowTestObjectsMultiSet expectedTestRowMultiSet =
        createExpectedTestRowMultiSet(testDesc, testData);

    // UNDONE: Inner count
    System.out.println("*BENCHMARK* expectedTestRowMultiSet rowCount " + expectedTestRowMultiSet.getRowCount() +
        " totalCount " + expectedTestRowMultiSet.getTotalCount());

    // Execute all implementation variations.
    for (MapJoinTestImplementation mapJoinImplementation : MapJoinTestImplementation.values()) {
      executeTestImplementation(mapJoinImplementation, testDesc, testData,
          expectedTestRowMultiSet);
    }
  }

  private boolean isVectorOutput(MapJoinTestImplementation mapJoinImplementation) {
    return
        (mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_HASH_MAP &&
         mapJoinImplementation != MapJoinTestImplementation.ROW_MODE_OPTIMIZED);
  }

  private void executeTestImplementation(
      MapJoinTestImplementation mapJoinImplementation,
      MapJoinTestDescription testDesc, MapJoinTestData testData, RowTestObjectsMultiSet expectedTestRowMultiSet)
          throws Exception {

    System.out.println("*BENCHMARK* Starting " + mapJoinImplementation + " test");

    // UNDONE: Parameterize for implementation variation?
    MapJoinDesc mapJoinDesc = MapJoinTestConfig.createMapJoinDesc(testDesc);

    final boolean isVectorOutput = isVectorOutput(mapJoinImplementation);

    RowTestObjectsMultiSet outputTestRowMultiSet = new RowTestObjectsMultiSet();

    Operator<? extends OperatorDesc> testCollectorOperator =
        (!isVectorOutput ?
            new TestMultiSetCollectorOperator(
                testDesc.outputObjectInspectors, outputTestRowMultiSet) :
            new TestMultiSetVectorCollectorOperator(
                testDesc.outputTypeInfos, testDesc.outputObjectInspectors, outputTestRowMultiSet));

    MapJoinOperator operator =
        MapJoinTestConfig.createMapJoinImplementation(
            mapJoinImplementation, testDesc, testCollectorOperator, testData, mapJoinDesc);

    if (!isVectorOutput) {
      MapJoinTestData.driveBigTableData(testDesc, testData, operator);
    } else {
      MapJoinTestData.driveVectorBigTableData(testDesc, testData, operator);
    }

    System.out.println("*BENCHMARK* executeTestImplementation row count " +
        ((CountCollectorTestOperator) testCollectorOperator).getRowCount());

    // Verify the output!
    if (!expectedTestRowMultiSet.verify(outputTestRowMultiSet)) {
      System.out.println("*BENCHMARK* verify failed for " + mapJoinImplementation);
    } else {
      System.out.println("*BENCHMARK* verify succeeded for " + mapJoinImplementation);
    }
  }
}