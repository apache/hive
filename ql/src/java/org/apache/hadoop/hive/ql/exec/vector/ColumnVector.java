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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 *
 * The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */
public abstract class ColumnVector {

  /*
   * If hasNulls is true, then this array contains true if the value
   * is null, otherwise false. The array is always allocated, so a batch can be re-used
   * later and nulls added.
   */
  public boolean[] isNull;

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;

  /*
   * True if same value repeats for whole column vector.
   * If so, vector[0] holds the repeating value.
   */
  public boolean isRepeating;
  public abstract Writable getWritableObject(int index);

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param len Vector length
   */
  public ColumnVector(int len) {
    isNull = new boolean[len];
    noNulls = true;
    isRepeating = false;
  }

  /**
     * Resets the column to default state
     *  - fills the isNull array with false
     *  - sets noNulls to true
     *  - sets isRepeating to false
     */
    public void reset() {
      if (false == noNulls) {
        Arrays.fill(isNull, false);
      }
      noNulls = true;
      isRepeating = false;
    }
  }

