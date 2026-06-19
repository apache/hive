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

/**
 * This class represents a void (or no) type column vector.
 *
 * No value vector(s) needed.  Always a NULL.
 */
public class VoidColumnVector extends ColumnVector {

  /**
   * Use this constructor by default. All column vectors
   * should normally be the default size.
   */
  public VoidColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len
   */
  public VoidColumnVector(int len) {
    super(Type.VOID, len);
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setElement(int outputElementNum, int inputElementNum,
      ColumnVector inputColVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copySelected(boolean selectedInUse, int[] sel, int size,
      ColumnVector outputColVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    throw new UnsupportedOperationException();
  }
}
