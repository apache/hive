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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import org.apache.hive.common.util.Murmur3;

import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A hash map key wrapper for vectorized processing.
 * It stores the key values as primitives in arrays for each supported primitive type.
 * This works in conjunction with
 * {@link org.apache.hadoop.hive.ql.exec.VectorHashKeyWrapperBatch VectorHashKeyWrapperBatch}
 * to hash vectorized processing units (batches).
 */
public abstract class VectorHashKeyWrapperBase extends KeyWrapper {

  public static final class HashContext {
    private final Murmur3.IncrementalHash32 bytesHash = new Murmur3.IncrementalHash32();

    public static Murmur3.IncrementalHash32 getBytesHash(HashContext ctx) {
      if (ctx == null) {
        return new Murmur3.IncrementalHash32();
      }
      return ctx.bytesHash;
    }
  }

  protected int hashcode;

  protected VectorHashKeyWrapperBase() {
    hashcode = 0;
  }

  @Override
  public void getNewKey(Object row, ObjectInspector rowInspector) throws HiveException {
    throw new HiveException("Should not be called");
  }

  @Override
  public void setHashKey() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public boolean equals(Object that) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  protected Object clone() {
    throw new RuntimeException("Not implemented");
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

  public void assignLong(int keyIndex, int index, long v) {
    throw new RuntimeException("Not implemented");
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  public void assignLong(int index, long v) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullLong(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  public void assignDouble(int index, double d) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullDouble(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  public void assignString(int index, byte[] bytes, int start, int length) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullString(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  public void assignDecimal(int index, HiveDecimalWritable value) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullDecimal(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  public void assignTimestamp(int index, Timestamp value) {
    throw new RuntimeException("Not implemented");
  }

  public void assignTimestamp(int index, TimestampColumnVector colVector, int elementNum) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullTimestamp(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  public void assignIntervalDayTime(int index, HiveIntervalDayTime value) {
    throw new RuntimeException("Not implemented");
  }

  public void assignIntervalDayTime(int index, IntervalDayTimeColumnVector colVector, int elementNum) {
    throw new RuntimeException("Not implemented");
  }

  public void assignNullIntervalDayTime(int keyIndex, int index) {
    throw new RuntimeException("Not implemented");
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public String toString()
  {
    throw new RuntimeException("Not implemented");
  }

  public long getLongValue(int i) {
    throw new RuntimeException("Not implemented");
  }

  public double getDoubleValue(int i) {
    throw new RuntimeException("Not implemented");
  }

  public byte[] getBytes(int i) {
    throw new RuntimeException("Not implemented");
  }

  public int getByteStart(int i) {
    throw new RuntimeException("Not implemented");
  }

  public int getByteLength(int i) {
    throw new RuntimeException("Not implemented");
  }

  public HiveDecimalWritable getDecimal(int i) {
    throw new RuntimeException("Not implemented");
  }

  public Timestamp getTimestamp(int i) {
    throw new RuntimeException("Not implemented");
  }

  public HiveIntervalDayTime getIntervalDayTime(int i) {
    throw new RuntimeException("Not implemented");
  }

  public int getVariableSize() {
    throw new RuntimeException("Not implemented");
  }

  public void clearIsNull() {
    throw new RuntimeException("Not implemented");
  }

  public void setNull() {
    throw new RuntimeException("Not implemented");
  }

  public boolean isNull(int keyIndex) {
    throw new RuntimeException("Not implemented");
  }
}
