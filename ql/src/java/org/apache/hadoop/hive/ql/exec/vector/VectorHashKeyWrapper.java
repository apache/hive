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

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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

  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private static final long[] EMPTY_LONG_ARRAY = new long[0];
  private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
  private static final byte[][] EMPTY_BYTES_ARRAY = new byte[0][];
  private static final HiveDecimalWritable[] EMPTY_DECIMAL_ARRAY = new HiveDecimalWritable[0];

  private long[] longValues;
  private double[] doubleValues;

  private byte[][] byteValues;
  private int[] byteStarts;
  private int[] byteLengths;

  private HiveDecimalWritable[] decimalValues;

  private boolean[] isNull;
  private int hashcode;

  public VectorHashKeyWrapper(int longValuesCount, int doubleValuesCount,
          int byteValuesCount, int decimalValuesCount) {
    longValues = longValuesCount > 0 ? new long[longValuesCount] : EMPTY_LONG_ARRAY;
    doubleValues = doubleValuesCount > 0 ? new double[doubleValuesCount] : EMPTY_DOUBLE_ARRAY;
    decimalValues = decimalValuesCount > 0 ? new HiveDecimalWritable[decimalValuesCount] : EMPTY_DECIMAL_ARRAY;
    for(int i = 0; i < decimalValuesCount; ++i) {
      decimalValues[i] = new HiveDecimalWritable(HiveDecimal.ZERO);
    }
    if (byteValuesCount > 0) {
      byteValues = new byte[byteValuesCount][];
      byteStarts = new int[byteValuesCount];
      byteLengths = new int[byteValuesCount];
    } else {
      byteValues = EMPTY_BYTES_ARRAY;
      byteStarts = EMPTY_INT_ARRAY;
      byteLengths = EMPTY_INT_ARRAY;
    }
    isNull = new boolean[longValuesCount + doubleValuesCount + byteValuesCount + decimalValuesCount];
    hashcode = 0;
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

    for (int i = 0; i < decimalValues.length; i++) {
      hashcode ^= decimalValues[i].getHiveDecimal().hashCode();
    }

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
          Arrays.equals(decimalValues,  keyThat.decimalValues) &&
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
    duplicateTo(clone);
    return clone;
  }

  public void duplicateTo(VectorHashKeyWrapper clone) {
    clone.longValues = (longValues.length > 0) ? longValues.clone() : EMPTY_LONG_ARRAY;
    clone.doubleValues = (doubleValues.length > 0) ? doubleValues.clone() : EMPTY_DOUBLE_ARRAY;
    clone.isNull = isNull.clone();

    if (decimalValues.length > 0) {
      // Decimal columns use HiveDecimalWritable.
      clone.decimalValues = new HiveDecimalWritable[decimalValues.length];
      for(int i = 0; i < decimalValues.length; ++i) {
        clone.decimalValues[i] = new HiveDecimalWritable(decimalValues[i]);
      }
    } else {
      clone.decimalValues = EMPTY_DECIMAL_ARRAY;
    }

    if (byteLengths.length > 0) {
      clone.byteValues = new byte[byteValues.length][];
      clone.byteStarts = new int[byteValues.length];
      clone.byteLengths = byteLengths.clone();
      for (int i = 0; i < byteValues.length; ++i) {
        // avoid allocation/copy of nulls, because it potentially expensive.
        // branch instead.
        if (!isNull[longValues.length + doubleValues.length + i]) {
          clone.byteValues[i] = Arrays.copyOfRange(byteValues[i],
              byteStarts[i], byteStarts[i] + byteLengths[i]);
        }
      }
    } else {
      clone.byteValues = EMPTY_BYTES_ARRAY;
      clone.byteStarts = EMPTY_INT_ARRAY;
      clone.byteLengths = EMPTY_INT_ARRAY;
    }
    clone.hashcode = hashcode;
    assert clone.equals(this);
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

  public void assignDecimal(int index, HiveDecimalWritable value) {
    decimalValues[index].set(value);
    isNull[longValues.length + doubleValues.length + byteValues.length + index] = false;
  }

  public void assignNullDecimal(int index) {
      isNull[longValues.length + doubleValues.length + byteValues.length + index] = true;
  }

  @Override
  public String toString()
  {
    return String.format("%d[%s] %d[%s] %d[%s] %d[%s]",
        longValues.length, Arrays.toString(longValues),
        doubleValues.length, Arrays.toString(doubleValues),
        byteValues.length, Arrays.toString(byteValues),
        decimalValues.length, Arrays.toString(decimalValues));
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

  public boolean getIsDecimalNull(int i) {
    return isNull[longValues.length + doubleValues.length + byteValues.length + i];
  }

  public HiveDecimalWritable getDecimal(int i) {
    return decimalValues[i];
  }
}

