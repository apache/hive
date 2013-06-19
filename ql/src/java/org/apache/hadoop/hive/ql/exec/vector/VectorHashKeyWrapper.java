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

import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A hash map key wrapper for vectorized processing.
 * It stores the key values as primitives in arrays for each supported primitive type.
 * This works in conjunction with
 * {@link org.apache.hadoop.hive.ql.exec.VectorHashKeyWrapperBatch VectorHashKeyWrapperBatch}
 * to hash vectorized processing units (batches).
 */
public class VectorHashKeyWrapper extends KeyWrapper {

  private long[] longValues;
  private double[] doubleValues;

  private byte[][] byteValues;
  private int[] byteStarts;
  private int[] byteLengths;

  private boolean[] isNull;
  private int hashcode;

  public VectorHashKeyWrapper(int longValuesCount, int doubleValuesCount, int byteValuesCount) {
    longValues = new long[longValuesCount];
    doubleValues = new double[doubleValuesCount];
    byteValues = new byte[byteValuesCount][];
    byteStarts = new int[byteValuesCount];
    byteLengths = new int[byteValuesCount];
    isNull = new boolean[longValuesCount + doubleValuesCount + byteValuesCount];
  }

  private VectorHashKeyWrapper() {
  }

  @Override
  public void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {
    throw new HiveException("Should not be called");
  }

  @Override
  public void setHashKey() {
    hashcode = Arrays.hashCode(longValues) ^
        Arrays.hashCode(doubleValues) ^
        Arrays.hashCode(isNull);

    // This code, with branches and all, is not executed if there are no string keys
    for (int i = 0; i < byteValues.length; ++i) {
      /*
       *  Hashing the string is potentially expensive so is better to branch.
       *  Additionally not looking at values for nulls allows us not reset the values.
       */
      if (!isNull[longValues.length + doubleValues.length + i]) {
        byte[] bytes = byteValues[i];
        int start = byteStarts[i];
        int length = byteLengths[i];
        if (length == bytes.length && start == 0) {
          hashcode ^= Arrays.hashCode(bytes);
        }
        else {
          // Unfortunately there is no Arrays.hashCode(byte[], start, length)
          for(int j = start; j < start + length; ++j) {
            // use 461 as is a (sexy!) prime.
            hashcode ^= 461 * bytes[j];
          }
        }
      }
    }
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapper) {
      VectorHashKeyWrapper keyThat = (VectorHashKeyWrapper)that;
      return hashcode == keyThat.hashcode &&
          Arrays.equals(longValues, keyThat.longValues) &&
          Arrays.equals(doubleValues, keyThat.doubleValues) &&
          Arrays.equals(isNull, keyThat.isNull) &&
          byteValues.length == keyThat.byteValues.length &&
          (0 == byteValues.length || bytesEquals(keyThat));
    }
    return false;
  }

  private boolean bytesEquals(VectorHashKeyWrapper keyThat) {
    //By the time we enter here the byteValues.lentgh and isNull must have already been compared
    for (int i = 0; i < byteValues.length; ++i) {
      // the byte comparison is potentially expensive so is better to branch on null
      if (!isNull[longValues.length + doubleValues.length + i]) {
        if (0 != StringExpr.compare(
            byteValues[i],
            byteStarts[i],
            byteLengths[i],
            keyThat.byteValues[i],
            keyThat.byteStarts[i],
            keyThat.byteLengths[i])) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapper clone = new VectorHashKeyWrapper();
    clone.longValues = longValues.clone();
    clone.doubleValues = doubleValues.clone();
    clone.isNull = isNull.clone();

    clone.byteValues = new byte[byteValues.length][];
    clone.byteStarts = new int[byteValues.length];
    clone.byteLengths = byteLengths.clone();
    for (int i = 0; i < byteValues.length; ++i) {
      // avoid allocation/copy of nulls, because it potentially expensive. branch instead.
      if (!isNull[longValues.length + doubleValues.length + i]) {
        clone.byteValues[i] = Arrays.copyOfRange(
            byteValues[i],
            byteStarts[i],
            byteStarts[i] + byteLengths[i]);
      }
    }
    clone.hashcode = hashcode;
    assert clone.equals(this);
    return clone;
  }

  @Override
  public KeyWrapper copyKey() {
    return (KeyWrapper) clone();
  }

  @Override
  public void copyKey(KeyWrapper oldWrapper) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] getKeyArray() {
    throw new UnsupportedOperationException();
  }

  public void assignDouble(int index, double d) {
    doubleValues[index] = d;
    isNull[longValues.length + index] = false;
  }

  public void assignNullDouble(int index) {
    doubleValues[index] = 0; // assign 0 to simplify hashcode
    isNull[longValues.length + index] = true;
  }

  public void assignLong(int index, long v) {
    longValues[index] = v;
    isNull[index] = false;
  }

  public void assignNullLong(int index) {
    longValues[index] = 0; // assign 0 to simplify hashcode
    isNull[index] = true;
  }

  public void assignString(int index, byte[] bytes, int start, int length) {
    byteValues[index] = bytes;
    byteStarts[index] = start;
    byteLengths[index] = length;
    isNull[longValues.length + doubleValues.length + index] = false;
  }

  public void assignNullString(int index) {
    // We do not assign the value to byteValues[] because the value is never used on null
    isNull[longValues.length + doubleValues.length + index] = true;
  }

  @Override
  public String toString()
  {
    return String.format("%d[%s] %d[%s] %d[%s]",
        longValues.length, Arrays.toString(longValues),
        doubleValues.length, Arrays.toString(doubleValues),
        byteValues.length, Arrays.toString(byteValues));
  }

  public boolean getIsLongNull(int i) {
    return isNull[i];
  }

  public boolean getIsDoubleNull(int i) {
    return isNull[longValues.length + i];
  }

  public boolean getIsBytesNull(int i) {
    return isNull[longValues.length + doubleValues.length + i];
  }

  
  public long getLongValue(int i) {
    return longValues[i];
  }

  public double getDoubleValue(int i) {
    return doubleValues[i];
  }

  public byte[] getBytes(int i) {
    return byteValues[i];
  }

  public int getByteStart(int i) {
    return byteStarts[i];
  }

  public int getByteLength(int i) {
    return byteLengths[i];
  }

  public int getVariableSize() {
    int variableSize = 0;
    for (int i=0; i<byteLengths.length; ++i) {
      JavaDataModel model = JavaDataModel.get();
      variableSize += model.lengthForByteArrayOfSize(byteLengths[i]);
    }
    return variableSize;
  }


}

