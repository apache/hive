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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;


/**
 * Generate random batch source from a random Object[] row source (VectorRandomRowSource).
 */
public class VectorRandomBatchSource {

  // Divide up rows array into different sized batches.
  // Modify the rows array for isRepeating / NULL patterns.
  // Provide iterator that will fill up a VRB with the divided up rows.

  private final VectorRandomRowSource vectorRandomRowSource;

  private final Object[][] randomRows;

  private final int rowCount;
  private final int columnCount;

  private final VectorBatchPatterns vectorBatchPatterns;

  private VectorAssignRow vectorAssignRow;

  private int nextRowIndex;
  private int batchCount;

  private VectorRandomBatchSource(
      VectorRandomRowSource vectorRandomRowSource,
      Object[][] randomRows,
      VectorBatchPatterns vectorBatchPatterns,
      VectorAssignRow vectorAssignRow) {
    this.vectorRandomRowSource = vectorRandomRowSource;
    this.randomRows = randomRows;
    rowCount = randomRows.length;
    Object[] firstRow = randomRows[0];
    columnCount = firstRow.length;
    this.vectorBatchPatterns = vectorBatchPatterns;
    this.vectorAssignRow = vectorAssignRow;
  }

  public static class VectorRandomBatchParameters {
  }

  private static class VectorBatchPatterns {

    private final List<VectorBatchPattern> vectorBatchPatternList;

    VectorBatchPatterns(List<VectorBatchPattern> vectorBatchPatternList) {
      this.vectorBatchPatternList = vectorBatchPatternList;
    }

    List<VectorBatchPattern> getTectorBatchPatternList() {
      return vectorBatchPatternList;
    }
  }

  private static class VectorBatchPattern {

    final int batchSize;
    final BitSet bitSet;

    private VectorBatchPattern(int batchSize, BitSet bitSet) {
      this.batchSize = batchSize;
      this.bitSet = bitSet;
    }

    public static VectorBatchPattern createRegularBatch(int batchSize) {
      return new VectorBatchPattern(batchSize, null);
    }

    public static VectorBatchPattern createRepeatedBatch(int batchSize, BitSet bitSet) {
      return new VectorBatchPattern(batchSize, bitSet);
    }

    public int getBatchSize() {
      return batchSize;
    }

    public BitSet getBitSet() {
      return bitSet;
    }

    public String toString() {
      String batchSizeString = "batchSize " + Integer.toString(batchSize);
      if (bitSet == null) {
        return batchSizeString;
      }
      long bitMask = bitSet.toLongArray()[0];
      return batchSizeString + " repeating 0x" + Long.toHexString(bitMask);
    }
  }

  private static VectorBatchPatterns chooseBatchPatterns(
      Random random,
      VectorRandomRowSource vectorRandomRowSource,
      Object[][] randomRows) {

    List<VectorBatchPattern> vectorBatchPatternList = new ArrayList<VectorBatchPattern>();
    final int rowCount = randomRows.length;
    int rowIndex = 0;

    if (rowCount > 0) {

      final int columnCount = randomRows[0].length;

      // Choose first up to a full batch.
      final int regularBatchSize = Math.min(rowCount - rowIndex, VectorizedRowBatch.DEFAULT_SIZE);
      vectorBatchPatternList.add(VectorBatchPattern.createRegularBatch(regularBatchSize));
      rowIndex += regularBatchSize;

      // Have a non-NULL value on hand.
      Object[] nonNullRow = new Object[columnCount];
      for (int c = 0; c < columnCount; c++) {
        for (int r = 0; r < rowCount; r++) {
          Object object = randomRows[r][c];
          if (object != null) {
            nonNullRow[c] = object;
            break;
          }
        }
      }

      int columnPermutationLimit = Math.min(columnCount, Long.SIZE);

      // Repeated NULL permutations.
      long columnPermutation = 1;
      while (true) {
        if (columnPermutation > columnPermutationLimit) {
          break;
        }
        final int maximumRowCount = Math.min(rowCount - rowIndex, VectorizedRowBatch.DEFAULT_SIZE);
        if (maximumRowCount == 0) {
          break;
        }
        int randomRowCount = 1 + random.nextInt(maximumRowCount);
        final int rowLimit = rowIndex + randomRowCount;

        BitSet bitSet = BitSet.valueOf(new long[]{columnPermutation});

        for (int columnNum = bitSet.nextSetBit(0);
             columnNum >= 0;
             columnNum = bitSet.nextSetBit(columnNum + 1)) {

          // Repeated NULL fill down column.
          for (int r = rowIndex; r < rowLimit; r++) {
            randomRows[r][columnNum] = null;
          }
        }
        vectorBatchPatternList.add(VectorBatchPattern.createRepeatedBatch(randomRowCount, bitSet));
        columnPermutation++;
        rowIndex = rowLimit;
      }

      // Repeated non-NULL permutations.
      columnPermutation = 1;
      while (true) {
        if (columnPermutation > columnPermutationLimit) {
          break;
        }
        final int maximumRowCount = Math.min(rowCount - rowIndex, VectorizedRowBatch.DEFAULT_SIZE);
        if (maximumRowCount == 0) {
          break;
        }
        int randomRowCount = 1 + random.nextInt(maximumRowCount);
        final int rowLimit = rowIndex + randomRowCount;

        BitSet bitSet = BitSet.valueOf(new long[]{columnPermutation});

        for (int columnNum = bitSet.nextSetBit(0);
             columnNum >= 0;
             columnNum = bitSet.nextSetBit(columnNum + 1)) {

          // Repeated non-NULL fill down column.
          Object repeatedObject = randomRows[rowIndex][columnNum];
          if (repeatedObject == null) {
            repeatedObject = nonNullRow[columnNum];
          }
          for (int r = rowIndex; r < rowLimit; r++) {
            randomRows[r][columnNum] = repeatedObject;
          }
        }
        vectorBatchPatternList.add(VectorBatchPattern.createRepeatedBatch(randomRowCount, bitSet));
        columnPermutation++;
        rowIndex = rowLimit;
      }

      // Remaining batches.
      while (true) {
        final int maximumRowCount = Math.min(rowCount - rowIndex, VectorizedRowBatch.DEFAULT_SIZE);
        if (maximumRowCount == 0) {
          break;
        }
        int randomRowCount = 1 + random.nextInt(maximumRowCount);
        vectorBatchPatternList.add(VectorBatchPattern.createRegularBatch(randomRowCount));
        rowIndex += randomRowCount;
      }
    }

    // System.out.println("*DEBUG* vectorBatchPatternList" + vectorBatchPatternList.toString());

    return new VectorBatchPatterns(vectorBatchPatternList);
  }

  public static VectorRandomBatchSource createInterestingBatches(
      Random random,
      VectorRandomRowSource vectorRandomRowSource,
      Object[][] randomRows,
      VectorRandomBatchParameters vectorRandomBatchParameters)
          throws HiveException {

    VectorAssignRow vectorAssignRow = new VectorAssignRow();
    vectorAssignRow.init(vectorRandomRowSource.typeNames());

    VectorBatchPatterns vectorBatchPatterns =
        chooseBatchPatterns(random, vectorRandomRowSource, randomRows);

    return new VectorRandomBatchSource(
        vectorRandomRowSource, randomRows, vectorBatchPatterns, vectorAssignRow);
  }

  public VectorRandomRowSource getRowSource() {
    return vectorRandomRowSource;
  }

  public Object[][] getRandomRows() {
    return randomRows;
  }

  public void resetBatchIteration() {
    nextRowIndex = 0;
    batchCount = 0;
  }

  public int getBatchCount() {
    return batchCount;
  }

  public int getRowCount() {
    return rowCount;
  }

  /*
   * Patterns of isRepeating columns
   * For boolean: tri-state: null, 0, 1
   * For others: null, some-value
   * noNulls: sometimes false and there are no NULLs.
   * Random selectedInUse, too.
   */
  public boolean fillNextBatch(VectorizedRowBatch batch) {
    if (nextRowIndex >= rowCount) {
      return false;
    }

    VectorBatchPattern vectorBatchPattern =
        vectorBatchPatterns.getTectorBatchPatternList().get(batchCount);
    final int batchSize = vectorBatchPattern.getBatchSize();

    for (int c = 0; c < columnCount; c++) {
      batch.cols[c].reset();
    }

    BitSet bitSet = vectorBatchPattern.getBitSet();
    if (bitSet != null) {
      for (int columnNum = bitSet.nextSetBit(0);
           columnNum >= 0;
           columnNum = bitSet.nextSetBit(columnNum + 1)) {
        batch.cols[columnNum].isRepeating = true;
      }
    }

    int rowIndex = nextRowIndex;
    for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
      for (int c = 0; c < columnCount; c++) {
        if (batch.cols[c].isRepeating && batchIndex > 0) {
          continue;
        }
        vectorAssignRow.assignRowColumn(batch, batchIndex, c, randomRows[rowIndex][c]);
      }
      rowIndex++;
    }
    batch.size = batchSize;
    batchCount++;
    nextRowIndex += batchSize;
    return true;
  }
}
