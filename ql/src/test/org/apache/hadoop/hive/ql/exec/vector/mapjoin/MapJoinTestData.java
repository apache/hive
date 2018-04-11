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
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorRandomRowSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerator.GenerateType.GenerateCategory;
import org.apache.hadoop.hive.ql.exec.vector.util.batchgen.VectorBatchGenerateStream;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.MapJoinTestDescription.SmallTableGenerationParameters.ValueOption;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class MapJoinTestData {

  final long bigTableRandomSeed;
  final long smallTableRandomSeed;

  final GenerateType[] generateTypes;
  final VectorBatchGenerator generator;

  public final VectorizedRowBatch bigTableBatch;

  public final VectorBatchGenerateStream bigTableBatchStream;

  final SmallTableGenerationParameters smallTableGenerationParameters;

  HashMap<RowTestObjects, Integer> smallTableKeyHashMap;

  ArrayList<Integer> smallTableValueCounts;
  ArrayList<ArrayList<RowTestObjects>> smallTableValues;

  public MapJoinTestData(int rowCount, MapJoinTestDescription testDesc,
      long bigTableRandomSeed, long smallTableRandomSeed) throws HiveException {

    this.bigTableRandomSeed = bigTableRandomSeed;

    this.smallTableRandomSeed = smallTableRandomSeed;

    generateTypes = generateTypesFromTypeInfos(testDesc.bigTableTypeInfos);
    generator = new VectorBatchGenerator(generateTypes);

    bigTableBatch = generator.createBatch();

    // Add small table result columns.
    ColumnVector[] newCols = new ColumnVector[bigTableBatch.cols.length + testDesc.smallTableValueTypeInfos.length];
    System.arraycopy(bigTableBatch.cols, 0, newCols, 0, bigTableBatch.cols.length);

    for (int s = 0; s < testDesc.smallTableValueTypeInfos.length; s++) {
      newCols[bigTableBatch.cols.length + s] =
          VectorizedBatchUtil.createColumnVector(testDesc.smallTableValueTypeInfos[s]);
    }
    bigTableBatch.cols = newCols;
    bigTableBatch.numCols = newCols.length;
 
    // This stream will be restarted with the same random seed over and over.
    bigTableBatchStream = new VectorBatchGenerateStream(
        bigTableRandomSeed, generator, rowCount);

    VectorExtractRow vectorExtractRow = new VectorExtractRow();
    vectorExtractRow.init(testDesc.bigTableKeyTypeInfos);

    smallTableGenerationParameters = testDesc.getSmallTableGenerationParameters();

    smallTableKeyHashMap = new HashMap<RowTestObjects, Integer>();
    Random smallTableRandom = new Random(smallTableRandomSeed);
                                                  // Start small table random generation
                                                  // from beginning.

    ValueOption valueOption = smallTableGenerationParameters.getValueOption();
    int keyOutOfAThousand = smallTableGenerationParameters.getKeyOutOfAThousand();

    bigTableBatchStream.reset();
    while (bigTableBatchStream.isNext()) {
      bigTableBatch.reset();
      bigTableBatchStream.fillNext(bigTableBatch);

      final int size = bigTableBatch.size;
      for (int i = 0; i < size; i++) {

        if (smallTableRandom.nextInt(1000) <= keyOutOfAThousand) {

          RowTestObjects testKey = getTestKey(bigTableBatch, i, vectorExtractRow,
              testDesc.bigTableKeyTypeInfos.length,
              testDesc.bigTableObjectInspectors);

          if (valueOption == ValueOption.ONLY_ONE) {
            if (smallTableKeyHashMap.containsKey(testKey)) {
              continue;
            }
          }
          smallTableKeyHashMap.put((RowTestObjects) testKey.clone(), -1);
        }
      }
    }

    //---------------------------------------------------------------------------------------------

    // UNDONE: For now, don't add more small keys...
    /*
    // Add more small table keys that are not in Big Table batches.
    final int smallTableAdditionalLength = 1 + random.nextInt(4);
    final int smallTableAdditionalSize = smallTableAdditionalLength * maxBatchSize;
    VectorizedRowBatch[] smallTableAdditionalBatches = createBigTableBatches(generator, smallTableAdditionalLength);
    for (int i = 0; i < smallTableAdditionalLength; i++) {
      generator.generateBatch(smallTableAdditionalBatches[i], random, maxBatchSize);
    }
    TestRow[] additionalTestKeys = getTestKeys(smallTableAdditionalBatches, vectorExtractRow,
        testDesc.bigTableKeyTypeInfos.length, testDesc.bigTableObjectInspectors);
    final int smallTableAdditionKeyProbes = smallTableAdditionalSize / 2;
    for (int i = 0; i < smallTableAdditionKeyProbes; i++) {
      int index = random.nextInt(smallTableAdditionalSize);
      TestRow additionalTestKey = additionalTestKeys[index];
      smallTableKeyHashMap.put((TestRow) additionalTestKey.clone(), -1);
    }
    */

    // Number the test rows with collection order.
    int addCount = 0;
    for (Entry<RowTestObjects, Integer> testRowEntry : smallTableKeyHashMap.entrySet()) {
      testRowEntry.setValue(addCount++);
    }

    generateVariationData(this, testDesc, smallTableRandom);
  }

  public VectorBatchGenerateStream getBigTableBatchStream() {
    return bigTableBatchStream;
  }

  public VectorizedRowBatch getBigTableBatch() {
    return bigTableBatch;
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
    vectorExtractRow.init(testDesc.bigTableKeyTypeInfos);

    final int columnCount = testDesc.bigTableKeyTypeInfos.length;
    Object[] row = new Object[columnCount];

    testData.bigTableBatchStream.reset();
    while (testData.bigTableBatchStream.isNext()) {
      testData.bigTableBatch.reset();
      testData.bigTableBatchStream.fillNext(testData.bigTableBatch);

      // Extract rows and call process per row
      final int size = testData.bigTableBatch.size;
      for (int r = 0; r < size; r++) {
        vectorExtractRow.extractRow(testData.bigTableBatch, r, row);
        operator.process(row, 0);
      }
    }
    operator.closeOp(false);
  }

  public static void driveVectorBigTableData(MapJoinTestDescription testDesc, MapJoinTestData testData,
      MapJoinOperator operator) throws HiveException {

    testData.bigTableBatchStream.reset();
    while (testData.bigTableBatchStream.isNext()) {
      testData.bigTableBatch.reset();
      testData.bigTableBatchStream.fillNext(testData.bigTableBatch);

      operator.process(testData.bigTableBatch, 0);
    }
    operator.closeOp(false);
  }

  public static void generateVariationData(MapJoinTestData testData,
      MapJoinTestDescription testDesc, Random random) {
    switch (testDesc.vectorMapJoinVariation) {
    case INNER_BIG_ONLY:
    case LEFT_SEMI:
      testData.generateRandomSmallTableCounts(testDesc, random);
      break;
    case INNER:
    case OUTER:
      testData.generateRandomSmallTableCounts(testDesc, random);
      testData.generateRandomSmallTableValues(testDesc, random);
      break;
    default:
      throw new RuntimeException("Unknown operator variation " + testDesc.vectorMapJoinVariation);
    }
  }

  private static RowTestObjects generateRandomSmallTableValueRow(MapJoinTestDescription testDesc, Random random) {

    final int columnCount = testDesc.smallTableValueTypeInfos.length;
    Object[] smallTableValueRow = VectorRandomRowSource.randomWritablePrimitiveRow(columnCount, random,
        testDesc.smallTableValuePrimitiveTypeInfos);
    for (int c = 0; c < smallTableValueRow.length; c++) {
      smallTableValueRow[c] = ((PrimitiveObjectInspector) testDesc.smallTableObjectInspectors[c]).copyObject(smallTableValueRow[c]);
    }
    return new RowTestObjects(smallTableValueRow);
  }

  private void generateRandomSmallTableCounts(MapJoinTestDescription testDesc, Random random) {
    smallTableValueCounts = new ArrayList<Integer>();
    for (Entry<RowTestObjects, Integer> testKeyEntry : smallTableKeyHashMap.entrySet()) {
      final int valueCount = 1 + random.nextInt(19);
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

  private static GenerateType[] generateTypesFromTypeInfos(TypeInfo[] typeInfos) {
    final int size = typeInfos.length;
    GenerateType[] generateTypes = new GenerateType[size];
    for (int i = 0; i < size; i++) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfos[i];
      GenerateCategory category =
          GenerateCategory.generateCategoryFromPrimitiveCategory(primitiveTypeInfo.getPrimitiveCategory());
      generateTypes[i] = new GenerateType(category);
    }
    return generateTypes;
  }
}