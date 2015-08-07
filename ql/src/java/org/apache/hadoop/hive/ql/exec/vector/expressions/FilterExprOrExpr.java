/**
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

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class represents an Or expression. This applies short circuit optimization.
 */
public class FilterExprOrExpr extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private transient final int[] initialSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient int[] unselected = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient int[] unselectedCopy = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient int[] difference = new int[VectorizedRowBatch.DEFAULT_SIZE];
  private transient final int[] tmp = new int[VectorizedRowBatch.DEFAULT_SIZE];

  public FilterExprOrExpr() {
    super();
  }

  /**
   * Remove (subtract) members from an array and produce the results into
   * a difference array.

   * @param all
   *          The selected array containing all members.
   * @param allSize
   *          The size of all.
   * @param remove
   *          The indices to remove.  They must all be present in input selected array.
   * @param removeSize
   *          The size of remove.
   * @param difference
   *          The resulting difference -- the all array indices not in the
   *          remove array.
   * @return
   *          The resulting size of the difference array.
   */
  private int subtract(int[] all, int allSize,
      int[] remove, int removeSize, int[] difference) {

    // UNDONE: Copied from VectorMapJoinOuterGenerateResultOperator.

    Preconditions.checkState((all != remove) && (remove != difference) && (difference != all));
    
    // Comment out these checks when we are happy..
    if (!verifyMonotonicallyIncreasing(all, allSize)) {
      throw new RuntimeException("all is not in sort order and unique");
    }
    if (!verifyMonotonicallyIncreasing(remove, removeSize)) {
      throw new RuntimeException("remove is not in sort order and unique");
    }

    int differenceCount = 0;

    // Determine which rows are left.
    int removeIndex = 0;
    for (int i = 0; i < allSize; i++) {
      int candidateIndex = all[i];
      if (removeIndex < removeSize && candidateIndex == remove[removeIndex]) {
        removeIndex++;
      } else {
        difference[differenceCount++] = candidateIndex;
      }
    }

    if (removeIndex != removeSize) {
      throw new RuntimeException("Not all batch indices removed");
    }

    if (!verifyMonotonicallyIncreasing(difference, differenceCount)) {
      throw new RuntimeException("difference is not in sort order and unique");
    }

    return differenceCount;
  }

  public boolean verifyMonotonicallyIncreasing(int[] selected, int size) {

    if (size == 0) {
      return true;
    }
    int prevBatchIndex = selected[0];

    for (int i = 1; i < size; i++) {
      int batchIndex = selected[i];
      if (batchIndex <= prevBatchIndex) {
        return false;
      }
      prevBatchIndex = batchIndex;
    }
    return true;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    int n = batch.size;
    if (n <= 0) {
      return;
    }

    VectorExpression childExpr1 = this.childExpressions[0];

    boolean prevSelectInUse = batch.selectedInUse;

    // Save the original selected vector
    int[] sel = batch.selected;
    if (batch.selectedInUse) {
      System.arraycopy(sel, 0, initialSelected, 0, n);
    } else {
      for (int i = 0; i < n; i++) {
        initialSelected[i] = i;
        sel[i] = i;
      }
      batch.selectedInUse = true;
    }

    childExpr1.evaluate(batch);

    // Preserve the selected reference and size values generated
    // after the first child is evaluated.
    int sizeAfterFirstChild = batch.size;
    int[] selectedAfterFirstChild = batch.selected;

    // Calculate unselected ones in last evaluate.
    for (int j = 0; j < n; j++) {
      tmp[initialSelected[j]] = 0;
    }
    for (int j = 0; j < batch.size; j++) {
      tmp[selectedAfterFirstChild[j]] = 1;
    }
    int unselectedSize = 0;
    for (int j = 0; j < n; j++) {
      int i = initialSelected[j];
      if (tmp[i] == 0) {
        unselected[unselectedSize++] = i;
      }
    }

    int newSize = sizeAfterFirstChild;

    batch.selected = unselected;
    batch.size = unselectedSize;

    if (unselectedSize > 0) {

      // Evaluate subsequent child expression over unselected ones only.

      final int childrenCount = this.childExpressions.length;
      int childIndex = 1;
      while (true) {
  
        boolean isLastChild = (childIndex + 1 >= childrenCount);
  
        // When we have yet another child beyond the current one... save unselected.
        if (!isLastChild) {
          System.arraycopy(batch.selected, 0, unselectedCopy, 0, unselectedSize);
        }
  
        VectorExpression childExpr = this.childExpressions[childIndex];
  
        childExpr.evaluate(batch);
  
        // Merge the result of last evaluate to previous evaluate.
        newSize += batch.size;
        for (int i = 0; i < batch.size; i++) {
          tmp[batch.selected[i]] = 1;
        }

        if (isLastChild) {
          break;
        }

        unselectedSize = subtract(unselectedCopy, unselectedSize, batch.selected, batch.size,
            difference);
        if (unselectedSize == 0) {
          break;
        }
        System.arraycopy(difference, 0, batch.selected, 0, unselectedSize);
        batch.size = unselectedSize;

        childIndex++;
      }
    }

    // Important: Restore the batch's selected array.
    batch.selected = selectedAfterFirstChild;

    int k = 0;
    for (int j = 0; j < n; j++) {
      int i = initialSelected[j];
      if (tmp[i] == 1) {
        batch.selected[k++] = i;
      }
    }

    batch.size = newSize;
    if (newSize == n) {
      // Filter didn't do anything
      batch.selectedInUse = prevSelectInUse;
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {

    // IMPORTANT NOTE: For Multi-OR, the VectorizationContext class will catch cases with 3 or
    //                 more parameters...

    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
