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

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * This class represents a nullable double precision floating point column vector.
 * This class will be used for operations on all floating point types (float, double)
 * and as such will use a 64-bit double value to hold the biggest possible value.
 * During copy-in/copy-out, smaller types (i.e. float) will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the
 * machine operates with 64-bit words.
 *
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class DoubleColumnVector extends ColumnVector {
  public double[] vector;
  private final DoubleWritable writableObj = new DoubleWritable();
  public static final double NULL_VALUE = Double.NaN;

  /**
   * Use this constructor by default. All column vectors
   * should normally be the default size.
   */
  public DoubleColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't use this except for testing purposes.
   *
   * @param len
   */
  public DoubleColumnVector(int len) {
    super(len);
    vector = new double[len];
  }

  @Override
  public Writable getWritableObject(int index) {
    if (this.isRepeating) {
      index = 0;
    }
    if (!noNulls && isNull[index]) {
      return NullWritable.get();
    } else {
      writableObj.set(vector[index]);
      return writableObj;
    }
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, DoubleColumnVector output) {

    // Output has nulls if and only if input has nulls.
    output.noNulls = noNulls;
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      output.vector[0] = vector[0];
      output.isNull[0] = isNull[0];
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    // Copy data values over
    if (selectedInUse) {
      for (int j = 0; j < size; j++) {
        int i = sel[j];
        output.vector[i] = vector[i];
      }
    }
    else {
      System.arraycopy(vector, 0, output.vector, 0, size);
    }

    // Copy nulls over if needed
    if (!noNulls) {
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          output.isNull[i] = isNull[i];
        }
      }
      else {
        System.arraycopy(isNull, 0, output.isNull, 0, size);
      }
    }
  }

  // Fill the column vector with the provided value
  public void fill(double value) {
    noNulls = true;
    isRepeating = true;
    vector[0] = value;
  }

  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      double repeatVal = vector[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          vector[i] = repeatVal;
        }
      } else {
        Arrays.fill(vector, 0, size, repeatVal);
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }
}
