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

/**
 * A representation of a Filter applied on the rows of a VectorizedRowBatch
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 *
 * Each FilterContext consists of an array with the ids (int) of rows that are selected
 * by the filter, an integer representing the number of selected rows, and a boolean showing
 * if the filter actually selected any rows.
 *
 */
public class FilterContext {
  private boolean currBatchIsSelectedInUse = false;
  private int[] currBatchSelected = null;
  private int currBatchSelectedSize = 0;

  /**
   * Empty constructor
   */
  public FilterContext(){};

  /**
   * Update context with the given values
   * @param isSelectedInUse if the filter is applied
   * @param selected an array of the selected rows
   * @param selectedSize the number of the selected rows
   */
  public void updateFilterContext(boolean isSelectedInUse, int[] selected, int selectedSize) {
    this.currBatchIsSelectedInUse = isSelectedInUse;
    this.currBatchSelected = selected;
    this.currBatchSelectedSize = selectedSize;
  }

  /**
   * Copy context variables from the a given FilterContext
   * @param other FilterContext to copy from
   */
  public void copyFilterContextFromOther(FilterContext other) {
    this.currBatchIsSelectedInUse = other.currBatchIsSelectedInUse;
    this.currBatchSelected = other.currBatchSelected;
    this.currBatchSelectedSize = other.currBatchSelectedSize;
  }

  /**
   * Reset FilterContext variables
   */
  public void resetFilterContext() {
    this.currBatchIsSelectedInUse = false;
    this.currBatchSelected = null;
    this.currBatchSelectedSize = 0;
  }

  /**
   * Set the selectedInUse boolean showing if the filter is applied
   * @param selectedInUse
   */
  public void setSelectedInUse(boolean selectedInUse) {
    this.currBatchIsSelectedInUse = selectedInUse;
  }

  /**
   * Is the filter applied?
   * @return true if the filter is actually applied
   */
  public boolean isSelectedInUse() {
    return this.currBatchIsSelectedInUse;
  }

  /**
   * Set the array of the rows that pass the filter
   * @param selectedArray
   */
  public void setSelected(int[] selectedArray) {
    this.currBatchSelected = selectedArray;
  }

  /**
   * Return an int array with the rows that pass the filter
   * @return int array
   */
  public int[] getSelected() {
    return this.currBatchSelected;
  }

  /**
   * Set the number of the rows that pass the filter
   * @param selectedSize
   */
  public void setSelectedSize(int selectedSize) {
    this.currBatchSelectedSize = selectedSize;
  }

  /**
   * Return the number of rows that pass the filter
   * @return an int
   */
  public int getSelectedSize() {
    return this.currBatchSelectedSize;
  }
}
