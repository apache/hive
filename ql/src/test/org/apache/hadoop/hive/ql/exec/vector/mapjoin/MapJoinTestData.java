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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomBatchSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource.GenerationSpec;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters.ValueOption;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.VectorMapJoinVariation;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class MapJoinTestData {

  final Random random;

  final List<GenerationSpec> generationSpecList;
  final VectorRandomRowSource bigTableRowSource;
  final Object[][] bigTableRandomRows;
  final VectorRandomBatchSource bigTableBatchSource;

  public final VectorizedRowBatch bigTableBatch;

  final SmallTableGenerationParameters smallTableGenerationParameters;

  HashMap<RowTestObjects, Integer> smallTableKeyHashMap;

  List<RowTestObjects> fullOuterAdditionalSmallTableKeys;

  ArrayList<Integer> smallTableValueCounts;
  ArrayList<ArrayList<RowTestObjects>> smallTableValues;

  public MapJoinTestData(int rowCount, MapJoinTestDescription testDesc,
      long randomSeed) throws HiveException {

    random = new Random(randomSeed);

    boolean isOuterJoin =
        (testDesc.vectorMapJoinVariation == VectorMapJoinVariation.OUTER ||
         testDesc.vectorMapJoinVariation == VectorMapJoinVariation.FULL_OUTER);

    generationSpecList = generationSpecListFromTypeInfos(
        testDesc.bigTableTypeInfos,
        testDesc.bigTableKeyColumnNums.length,
        isOuterJoin);

    bigTableRowSource = new VectorRandomRowSource();

    bigTableRowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true, null);

    // UNDONE: 100000
    bigTableRandomRows = bigTableRowSource.randomRows(10);

    bigTableBatchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            bigTableRowSource,
            bigTableRandomRows,
            null);

    bigTableBatch = createBigTableBatch(testDesc);

    // Add small table result columns.

    // Only [FULL] OUTER MapJoin needs a physical column.
    final int smallTableRetainKeySize =
        (isOuterJoin ? testDesc.smallTableRetainKeyColumnNums.length : 0);
    ColumnVector[] newCols =
        new ColumnVector[
            bigTableBatch.cols.length +
            smallTableRetainKeySize +
            testDesc.smallTableValueTypeInfos.length];
    System.arraycopy(bigTableBatch.cols, 0, newCols, 0, bigTableBatch.cols.length);

    int colIndex = bigTableBatch.cols.length;

    if (isOuterJoin) {
      for (int s = 0; s < smallTableRetainKeySize; s++) {
        final int smallTableKeyColumnNum = testDesc.smallTableRetainKeyColumnNums[s];
        newCols[colIndex++] =
            VectorizedBatchUtil.createColumnVector(
                testDesc.smallTableKeyTypeInfos[smallTableKeyColumnNum]);
      }
    }
    for (int s = 0; s < testDesc.smallTableValueTypeInfos.length; s++) {
      newCols[colIndex++] =
          VectorizedBatchUtil.createColumnVector(testDesc.smallTableValueTypeInfos[s]);
    }
    bigTableBatch.cols = newCols;
    bigTableBatch.numCols = newCols.length;

    VectorExtractRow keyVectorExtractRow = new VectorExtractRow();
    keyVectorExtractRow.init(testDesc.bigTableKeyTypeInfos, testDesc.bigTableKeyColumnNums);

    smallTableGenerationParameters = testDesc.getSmallTableGenerationParameters();

    HashMap<RowTestObjects, Integer> bigTableKeyHashMap = new HashMap<RowTestObjects, Integer>();
    smallTableKeyHashMap = new HashMap<RowTestObjects, Integer>();

    ValueOption valueOption = smallTableGenerationParameters.getValueOption();
    if (valueOption != ValueOption.NO_REGULAR_SMALL_KEYS) {
      int keyOutOfAThousand = smallTableGenerationParameters.getKeyOutOfAThousand();

      bigTableBatchSource.resetBatchIteration();
      while (bigTableBatchSource.fillNextBatch(bigTableBatch)) {

        final int size = bigTableBatch.size;
        for (int logical = 0; logical < size; logical++) {
          final int batchIndex =
              (bigTableBatch.selectedInUse ? bigTableBatch.selected[logical] : logical);

          RowTestObjects testKey = getTestKey(bigTableBatch, batchIndex, keyVectorExtractRow,
              testDesc.bigTableKeyTypeInfos.length,
              testDesc.bigTableObjectInspectors);
          bigTableKeyHashMap.put((RowTestObjects) testKey.clone(), -1);

          if (random.nextInt(1000) <= keyOutOfAThousand) {

            if (valueOption == ValueOption.ONLY_ONE) {
              if (smallTableKeyHashMap.containsKey(testKey)) {
                continue;
              }
            }
            smallTableKeyHashMap.put((RowTestObjects) testKey.clone(), -1);
          }
        }
      }
    }

    //---------------------------------------------------------------------------------------------

    // Add more small table keys that are not in Big Table or Small Table for FULL OUTER.

    fullOuterAdditionalSmallTableKeys = new ArrayList<RowTestObjects>();

    VectorRandomRowSource altBigTableRowSource = new VectorRandomRowSource();

    altBigTableRowSource.initGenerationSpecSchema(
        random, generationSpecList, /* maxComplexDepth */ 0,
        /* allowNull */ true, /* isUnicodeOk */ true, null);

    Object[][] altBigTableRandomRows = altBigTableRowSource.randomRows(10000);

    VectorRandomBatchSource altBigTableBatchSource =
        VectorRandomBatchSource.createInterestingBatches(
            random,
            altBigTableRowSource,
            altBigTableRandomRows,
            null);

    altBigTableBatchSource.resetBatchIteration();
    while (altBigTableBatchSource.fillNextBatch(bigTableBatch)) {
      final int size = bigTableBatch.size;
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex =
            (bigTableBatch.selectedInUse ? bigTableBatch.selected[logical] : logical);
        RowTestObjects testKey = getTestKey(bigTableBatch, batchIndex, keyVectorExtractRow,
            testDesc.bigTableKeyTypeInfos.length,
            testDesc.bigTableObjectInspectors);
        if (bigTableKeyHashMap.containsKey(testKey) ||
            smallTableKeyHashMap.containsKey(testKey)) {
          continue;
        }
        RowTestObjects testKeyClone = (RowTestObjects) testKey.clone();
        smallTableKeyHashMap.put(testKeyClone, -1);
        fullOuterAdditionalSmallTableKeys.add(testKeyClone);
      }
    }

    // Make sure there is a NULL key.
    Object[] nullKeyRowObjects = new Object[testDesc.bigTableKeyTypeInfos.length];
    RowTestObjects nullTestKey = new RowTestObjects(nullKeyRowObjects);
    if (!smallTableKeyHashMap.containsKey(nullTestKey)) {
      smallTableKeyHashMap.put(nullTestKey, -1);
      fullOuterAdditionalSmallTableKeys.add(nullTestKey);
    }

    // Number the test rows with collection order.
    int addCount = 0;
    for (Entry<RowTestObjects, Integer> testRowEntry : smallTableKeyHashMap.entrySet()) {
      testRowEntry.setValue(addCount++);
    }

    generateVariationData(this, testDesc, random);
  }

  public VectorRandomBatchSource getBigTableBatchSource() {
    return bigTableBatchSource;
  }

  public VectorizedRowBatch getBigTableBatch() {
    return bigTableBatch;
  }

  public VectorizedRowBatch createBigTableBatch(MapJoinTestDescription testDesc) {
    final int bigTableColumnCount = testDesc.bigTableTypeInfos.length;
    VectorizedRowBatch batch = new VectorizedRowBatch(bigTableColumnCount);
    for (int i = 0; i < bigTableColumnCount; i++) {
      batch.cols[i] =
          VectorizedBatchUtil.createColumnVector(
              testDesc.bigTableTypeInfos[i]);
    }
    return batch;
  }

  private RowTestObjects getTestKey(VectorizedRowBatch bigTableBatch, int batchIndex,
      VectorExtractRow vectorExtractRow, int columnCount, ObjectInspector[] objectInspectors) {
    Object[] rowObjects = new Object[columnCount];
    vectorExtractRow.extractRow(bigTableBatch, batchIndex, rowObjects);
    for (int c = 0; c < rowObjects.length; c++) {
      rowObjects[c] = ((PrimitiveObjectInspector) objectInspectors[c]).copyObject(rowObjects[c]);
    }
    return new RowTestObjects(rowObjects);
  }

  public static void driveBigTableData(MapJoinTestDescription testDesc, MapJoinTestData testData,
      MapJoinOperator operator) throws HiveException {

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(testDesc.bigTableTypeInfos);

    Object[][] bigTableRandomRows = testData.bigTableRandomRows;
    final int rowCount = bigTableRandomRows.length;
    for (int i = 0; i < rowCount; i++) {
      Object[] row = bigTableRandomRows[i];
      operator.process(row, 0);
    }

    // Close the operator tree.
    operator.close(false);
  }

  public static void driveVectorBigTableData(MapJoinTestDescription testDesc, MapJoinTestData testData,
      MapJoinOperator operator) throws HiveException {

    testData.bigTableBatchSource.resetBatchIteration();
    while (testData.bigTableBatchSource.fillNextBatch(testData.bigTableBatch)) {
      operator.process(testData.bigTableBatch, 0);
    }

    // Close the operator tree.
    operator.close(false);
  }

  public static void generateVariationData(MapJoinTestData testData,
      MapJoinTestDescription testDesc, Random random) {
    switch (testDesc.vectorMapJoinVariation) {
    case INNER_BIG_ONLY:
    case LEFT_SEMI:
    case LEFT_ANTI:
      testData.generateRandomSmallTableCounts(testDesc, random);
      break;
    case INNER:
    case OUTER:
    case FULL_OUTER:
      testData.generateRandomSmallTableCounts(testDesc, random);
      testData.generateRandomSmallTableValues(testDesc, random);
      break;
    default:
      throw new RuntimeException("Unknown operator variation " + testDesc.vectorMapJoinVariation);
    }
  }

  private static RowTestObjects generateRandomSmallTableValueRow(MapJoinTestDescription testDesc, Random random) {

    final int columnCount = testDesc.smallTableValueTypeInfos.length;
    PrimitiveTypeInfo[] primitiveTypeInfos = new PrimitiveTypeInfo[columnCount];
    for (int i = 0; i < columnCount; i++) {
      primitiveTypeInfos[i] = (PrimitiveTypeInfo) testDesc.smallTableValueTypeInfos[i];
    }
    Object[] smallTableValueRow =
        VectorRandomRowSource.randomWritablePrimitiveRow(
            columnCount, random, primitiveTypeInfos);
    for (int c = 0; c < smallTableValueRow.length; c++) {
      smallTableValueRow[c] = ((PrimitiveObjectInspector) testDesc.smallTableValueObjectInspectors[c]).copyObject(smallTableValueRow[c]);
    }
    return new RowTestObjects(smallTableValueRow);
  }

  private void generateRandomSmallTableCounts(MapJoinTestDescription testDesc, Random random) {
    smallTableValueCounts = new ArrayList<Integer>();
    for (Entry<RowTestObjects, Integer> testKeyEntry : smallTableKeyHashMap.entrySet()) {
      final int valueCount = 1 + random.nextInt(3);
      smallTableValueCounts.add(valueCount);
    }
  }

  private void generateRandomSmallTableValues(MapJoinTestDescription testDesc, Random random) {
    smallTableValues = new ArrayList<ArrayList<RowTestObjects>>();
    for (Entry<RowTestObjects, Integer> testKeyEntry : smallTableKeyHashMap.entrySet()) {
      ArrayList<RowTestObjects> valueList = new ArrayList<RowTestObjects>();
      smallTableValues.add(valueList);
      final int valueCount = smallTableValueCounts.get(testKeyEntry.getValue());
      for (int v = 0; v < valueCount; v++) {
        valueList.add(generateRandomSmallTableValueRow(testDesc, random));
      }
    }
  }

  private static List<GenerationSpec> generationSpecListFromTypeInfos(TypeInfo[] typeInfos,
      int keyCount, boolean isOuterJoin) {

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();

    final int size = typeInfos.length;
    for (int i = 0; i < size; i++) {
      TypeInfo typeInfo = typeInfos[i];
      final boolean columnAllowNulls;
      if (i >= keyCount) {

        // Value columns can be NULL.
        columnAllowNulls = true;
      } else {

        // Non-OUTER JOIN operators expect NULL keys to have been filtered out.
        columnAllowNulls = isOuterJoin;
      }
      generationSpecList.add(GenerationSpec.createSameType(typeInfo, columnAllowNulls));
    }
    return generationSpecList;
  }
}