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
package org.apache.hadoop.hive.ql.io.filter;

import java.util.Arrays;

/**
 * A representation of a Filter applied on the rows of a VectorizedRowBatch
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 *
 * Each FilterContext consists of an array with the ids (int) of rows that are selected by the
 * filter, an integer representing the number of selected rows, and a boolean showing if the filter
 * actually selected any rows.
 *
 */
public class MutableFilterContext extends FilterContext {

  /**
   * Set context with the given values by reference.
   * @param isSelectedInUse if the filter is applied
   * @param selected an array of the selected rows
   * @param selectedSize the number of the selected rows
   */
  public void setFilterContext(boolean isSelectedInUse, int[] selected, int selectedSize) {
    this.currBatchIsSelectedInUse = isSelectedInUse;
    this.currBatchSelected = selected;
    this.currBatchSelectedSize = selectedSize;
    // Avoid selected.length < selectedSize since we can borrow a larger array for selected
    // Debug loop for selected array: use without assert when needed (asserts only fail in testing)
    assert isValidSelected() : "Selected array may not contain duplicates or unordered values";
  }

  /**
   * Copy context variables from the a given FilterContext.
   * Always does a deep copy of the data.
   * @param other FilterContext to copy from
   */
  public void copyFilterContextFrom(MutableFilterContext other) {
    // assert if copying into self (can fail only in testing)
    assert this != other: "May not copy a FilterContext to itself";

    if (this != other) {
      if (this.currBatchSelected == null || this.currBatchSelected.length < other.currBatchSelectedSize) {
        // note: still allocating a full size buffer, for later use
        this.currBatchSelected = Arrays.copyOf(other.currBatchSelected, other.currBatchSelected.length);
      } else {
        System.arraycopy(other.currBatchSelected, 0, this.currBatchSelected, 0, other.currBatchSelectedSize);
      }
      this.currBatchSelectedSize = other.currBatchSelectedSize;
      this.currBatchIsSelectedInUse = other.currBatchIsSelectedInUse;
    }
  }

  /**
   * Validate method checking if existing selected array contains accepted values.
   * Values should be in order and without duplicates i.e [1,1,1] is illegal
   * @return true if the selected array is valid
   */
  public boolean isValidSelected() {
    for (int i = 1; i < this.currBatchSelectedSize; i++) {
      if (this.currBatchSelected[i-1] >= this.currBatchSelected[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Borrow the current selected array to be modified if it satisfies minimum capacity.
   * If it is too small or unset, allocates one.
   * This method never returns null!
   * @param minCapacity
   * @return the current selected array to be modified
   */
  public int[] borrowSelected(int minCapacity) {
    int[] existing = this.currBatchSelected;
    this.currBatchSelected = null;
    if (existing == null || existing.length < minCapacity) {
      return new int[minCapacity];
    }
    return existing;
  }

  /**
   * Get the immutable version of the current FilterContext.
   * @return immutable FilterContext instance
   */
  public FilterContext immutable(){
    return this;
  }

  /**
   * Set the selectedInUse boolean showing if the filter is applied.
   * @param selectedInUse
   */
  public void setSelectedInUse(boolean selectedInUse) {
    this.currBatchIsSelectedInUse = selectedInUse;
  }

  /**
   * Set the array of the rows that pass the filter by reference.
   * @param selectedArray
   */
  public void setSelected(int[] selectedArray) {
    this.currBatchSelected = selectedArray;
  }

  /**
   * Set the number of the rows that pass the filter.
   * @param selectedSize
   */
  public void setSelectedSize(int selectedSize) {
    this.currBatchSelectedSize = selectedSize;
  }
}
