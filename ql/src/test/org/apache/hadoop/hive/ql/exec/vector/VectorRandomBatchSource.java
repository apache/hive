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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

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
    final int[] selected;

    private VectorBatchPattern(Random random, int batchSize,
        BitSet bitSet, boolean asSelected) {
      this.batchSize = batchSize;
      this.bitSet = bitSet;
      if (asSelected) {
        selected = randomSelection(random, batchSize);
      } else {
        selected = null;
      }
    }

    private int[] randomSelection(Random random, int batchSize) {

      // Random batchSize unique ordered integers of 1024 (VectorizedRowBatch.DEFAULT_SIZE) indices.
      // This could be smarter...
      Set<Integer> selectedSet = new TreeSet<Integer>();
      int currentCount = 0;
      while (true) {
        final int candidateIndex = random.nextInt(VectorizedRowBatch.DEFAULT_SIZE);
        if (!selectedSet.contains(candidateIndex)) {
          selectedSet.add(candidateIndex);
          if (++currentCount == batchSize) {
            Integer[] integerArray = selectedSet.toArray(new Integer[0]);
            int[] result = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
              result[i] = integerArray[i];
            }
            return result;
          }
        }
      }
    }

    public static VectorBatchPattern createRegularBatch(int batchSize) {
      return new VectorBatchPattern(null, batchSize, null, false);
    }

    public static VectorBatchPattern createRegularBatch(Random random, int batchSize,
        boolean asSelected) {
      return new VectorBatchPattern(random, batchSize, null, asSelected);
    }

    public static VectorBatchPattern createRepeatedBatch(Random random, int batchSize,
        BitSet bitSet, boolean asSelected) {
      return new VectorBatchPattern(random, batchSize, bitSet, asSelected);
    }

    public int getBatchSize() {
      return batchSize;
    }

    public BitSet getBitSet() {
      return bitSet;
    }

    public int[] getSelected() {
      return selected;
    }

    public String toString() {
      String batchSizeString = "batchSize " + Integer.toString(batchSize);
      if (bitSet != null) {
        long bitMask = bitSet.toLongArray()[0];
        batchSizeString += " repeating 0x" + Long.toHexString(bitMask);
      }
      boolean selectedInUse = (selected != null);
      batchSizeString += " selectedInUse " + selectedInUse;
      if (selectedInUse) {
        batchSizeString += " selected " + Arrays.toString(selected);
      }
      return batchSizeString;
    }
  }

  private static VectorBatchPatterns chooseBatchPatterns(
      Random random,
      VectorRandomRowSource vectorRandomRowSource,
      Object[][] randomRows) {

    final boolean allowNull = vectorRandomRowSource.getAllowNull();

    List<VectorBatchPattern> vectorBatchPatternList = new ArrayList<VectorBatchPattern>();
    final int rowCount = randomRows.length;
    int rowIndex = 0;

    if (rowCount > 0) {

      final int columnCount = randomRows[0].length;

      // Choose first up to a full batch with no selection.
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

      boolean asSelected = false;

      /*
       * Do a round each as physical with no row selection and logical with row selection.
       */
      while (true) {

        long columnPermutation = 1;
        if (allowNull) {

          // Repeated NULL permutations.
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
            vectorBatchPatternList.add(
                VectorBatchPattern.createRepeatedBatch(
                    random, randomRowCount, bitSet, asSelected));
            columnPermutation++;
            rowIndex = rowLimit;
          }
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
          vectorBatchPatternList.add(
              VectorBatchPattern.createRepeatedBatch(
                  random, randomRowCount, bitSet, asSelected));
          columnPermutation++;
          rowIndex = rowLimit;
        }

        if (asSelected) {
          break;
        }
        asSelected = true;
      }

      // Remaining batches.
      while (true) {
        final int maximumRowCount = Math.min(rowCount - rowIndex, VectorizedRowBatch.DEFAULT_SIZE);
        if (maximumRowCount == 0) {
          break;
        }
        int randomRowCount = 1 + random.nextInt(maximumRowCount);
        asSelected = random.nextBoolean();
        vectorBatchPatternList.add(
            VectorBatchPattern.createRegularBatch(
                random, randomRowCount, asSelected));
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

    // System.out.println("*DEBUG* vectorBatchPattern " + vectorBatchPattern.toString());

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

    int[] selected = vectorBatchPattern.getSelected();
    boolean selectedInUse = (selected != null);
    batch.selectedInUse = selectedInUse;
    if (selectedInUse) {
      System.arraycopy(selected, 0, batch.selected, 0, batchSize);
    }

    int rowIndex = nextRowIndex;
    for (int logicalIndex = 0; logicalIndex < batchSize; logicalIndex++) {
      final int batchIndex = (selectedInUse ? selected[logicalIndex] : logicalIndex);
      for (int c = 0; c < columnCount; c++) {
        if (batch.cols[c].isRepeating) {
          if (logicalIndex > 0) {
            continue;
          }
          vectorAssignRow.assignRowColumn(batch, 0, c, randomRows[rowIndex][c]);
        } else {
          vectorAssignRow.assignRowColumn(batch, batchIndex, c, randomRows[rowIndex][c]);
        }
      }
      rowIndex++;
    }
    batch.size = batchSize;
    batchCount++;
    nextRowIndex += batchSize;
    return true;
  }
}
