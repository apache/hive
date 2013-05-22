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

package org.apache.hadoop.hive.ql.exec;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
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
  private boolean[] isNull;
  private int hashcode;
  
  public VectorHashKeyWrapper(int longValuesCount, int doubleValuesCount) {
    longValues = new long[longValuesCount];
    doubleValues = new double[doubleValuesCount];
    isNull = new boolean[longValuesCount + doubleValuesCount];
  }
  
  private VectorHashKeyWrapper() {
  }

  @Override
  void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {
    throw new HiveException("Should not be called");
  }

  @Override
  void setHashKey() {
    hashcode = Arrays.hashCode(longValues) ^
        Arrays.hashCode(doubleValues) ^
        Arrays.hashCode(isNull);
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
          Arrays.equals(isNull, keyThat.isNull);
    }
    return false;
  }
  
  @Override
  protected Object clone() {
    VectorHashKeyWrapper clone = new VectorHashKeyWrapper();
    clone.longValues = longValues.clone();
    clone.doubleValues = doubleValues.clone();
    clone.isNull = isNull.clone();
    clone.hashcode = hashcode;
    return clone;
  }

  @Override
  public KeyWrapper copyKey() {
    return (KeyWrapper) clone();
  }

  @Override
  void copyKey(KeyWrapper oldWrapper) {
    // TODO Auto-generated method stub

  }

  @Override
  Object[] getKeyArray() {
    // TODO Auto-generated method stub
    return null;
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
  
  @Override
  public String toString() 
  {
    return String.format("%d[%s] %d[%s]", 
        longValues.length, Arrays.toString(longValues),
        doubleValues.length, Arrays.toString(doubleValues));
  }

  public boolean getIsNull(int i) {
    return isNull[i];
  }
  
  public long getLongValue(int i) {
    return longValues[i];
  }

  public double getDoubleValue(int i) {
    return doubleValues[i - longValues.length];
  }

}
