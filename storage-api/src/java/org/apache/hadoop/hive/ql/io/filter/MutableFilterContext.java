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
 * Each FilterContext consists of an array with the ids (int) of rows that are selected by the
 * filter, an integer representing the number of selected rows, and a boolean showing if the filter
 * actually selected any rows.
 *
 */
public interface MutableFilterContext extends FilterContext {

  /**
   * Set context with the given values by reference.
   * @param isSelectedInUse if the filter is applied
   * @param selected an array of the selected rows
   * @param selectedSize the number of the selected rows
   */
  void setFilterContext(boolean isSelectedInUse, int[] selected, int selectedSize);

  /**
   * Validate method checking if existing selected array contains accepted values.
   * Values should be in order and without duplicates i.e [1,1,1] is illegal
   * @return true if the selected array is valid
   */
  boolean validateSelected();

  /**
   * Get an array for selected that is expected to be modified.
   * If it is too small or unset, allocates one.
   * This method never returns null!
   * @param minCapacity
   * @return the current selected array to be modified
   */
  int[] updateSelected(int minCapacity);

  /**
   * Get the immutable version of the current FilterContext.
   * @return immutable FilterContext instance
   */
  default FilterContext immutable() {
    return this;
  }

  /**
   * Set the selectedInUse boolean showing if the filter is applied.
   * @param selectedInUse
   */
  void setSelectedInUse(boolean selectedInUse);

  /**
   * Set the array of the rows that pass the filter by reference.
   * @param selectedArray
   */
  void setSelected(int[] selectedArray);

  /**
   * Set the number of the rows that pass the filter.
   * @param selectedSize
   */
  void setSelectedSize(int selectedSize);
}
