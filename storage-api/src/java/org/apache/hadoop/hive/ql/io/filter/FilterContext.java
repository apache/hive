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
public interface FilterContext {

  /**
   * Reset FilterContext variables.
   */
  void reset();

  /**
   * Is the filter applied?
   * @return true if the filter is actually applied
   */
  boolean isSelectedInUse();

  /**
   * Return an int array with the rows that pass the filter.
   * Do not modify the array returned!
   * @return int array
   */
  int[] getSelected();

  /**
   * Return the number of rows that pass the filter.
   * @return an int
   */
  int getSelectedSize();
}
