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

import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hive.common.util.Murmur3;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.KeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.base.Preconditions;

/**
 * A hash map key wrapper for vectorized processing.
 * It stores the key values as primitives in arrays for each supported primitive type.
 * This works in conjunction with
 * {@link org.apache.hadoop.hive.ql.exec.VectorHashKeyWrapperBatch VectorHashKeyWrapperBatch}
 * to hash vectorized processing units (batches).
 */
public class VectorHashKeyWrapperGeneral extends VectorHashKeyWrapperBase {

  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private static final long[] EMPTY_LONG_ARRAY = new long[0];
  private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
  private static final byte[][] EMPTY_BYTES_ARRAY = new byte[0][];
  private static final HiveDecimalWritable[] EMPTY_DECIMAL_ARRAY = new HiveDecimalWritable[0];
  private static final Timestamp[] EMPTY_TIMESTAMP_ARRAY = new Timestamp[0];
  private static final HiveIntervalDayTime[] EMPTY_INTERVAL_DAY_TIME_ARRAY = new HiveIntervalDayTime[0];

  private long[] longValues;
  private double[] doubleValues;

  private byte[][] byteValues;
  private int[] byteStarts;
  private int[] byteLengths;

  private HiveDecimalWritable[] decimalValues;

  private Timestamp[] timestampValues;
  private static Timestamp ZERO_TIMESTAMP = new Timestamp(0);

  private HiveIntervalDayTime[] intervalDayTimeValues;
  private static HiveIntervalDayTime ZERO_INTERVALDAYTIME= new HiveIntervalDayTime(0, 0);

  private HashContext hashCtx;

  private int keyCount;

  // NOTE: The null array is indexed by keyIndex, which is not available internally.  The mapping
  //       from a long, double, etc index to key index is kept once in the separate
  //       VectorColumnSetInfo object.
  protected boolean[] isNull;

  public VectorHashKeyWrapperGeneral(HashContext ctx, int longValuesCount, int doubleValuesCount,
          int byteValuesCount, int decimalValuesCount, int timestampValuesCount,
          int intervalDayTimeValuesCount,
          int keyCount) {
    super();
    hashCtx = ctx;
    this.keyCount = keyCount;
    longValues = longValuesCount > 0 ? new long[longValuesCount] : EMPTY_LONG_ARRAY;
    doubleValues = doubleValuesCount > 0 ? new double[doubleValuesCount] : EMPTY_DOUBLE_ARRAY;
    decimalValues = decimalValuesCount > 0 ? new HiveDecimalWritable[decimalValuesCount] : EMPTY_DECIMAL_ARRAY;
    timestampValues = timestampValuesCount > 0 ? new Timestamp[timestampValuesCount] : EMPTY_TIMESTAMP_ARRAY;
    intervalDayTimeValues = intervalDayTimeValuesCount > 0 ? new HiveIntervalDayTime[intervalDayTimeValuesCount] : EMPTY_INTERVAL_DAY_TIME_ARRAY;
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
    for(int i = 0; i < timestampValuesCount; ++i) {
      timestampValues[i] = new Timestamp(0);
    }
    for(int i = 0; i < intervalDayTimeValuesCount; ++i) {
      intervalDayTimeValues[i] = new HiveIntervalDayTime();
    }
    isNull = new boolean[keyCount];
  }

  private VectorHashKeyWrapperGeneral() {
    super();
  }

  @Override
  public void setHashKey() {
    // compute locally and assign
    int hash = Arrays.hashCode(longValues) ^
        Arrays.hashCode(doubleValues) ^
        Arrays.hashCode(isNull);

    for (int i = 0; i < decimalValues.length; i++) {
      // Use the new faster hash code since we are hashing memory objects.
      hash ^= decimalValues[i].newFasterHashCode();
    }

    for (int i = 0; i < timestampValues.length; i++) {
      hash ^= timestampValues[i].hashCode();
    }

    for (int i = 0; i < intervalDayTimeValues.length; i++) {
      hash ^= intervalDayTimeValues[i].hashCode();
    }

    // This code, with branches and all, is not executed if there are no string keys
    Murmur3.IncrementalHash32 bytesHash = null;
    for (int i = 0; i < byteValues.length; ++i) {
      /*
       *  Hashing the string is potentially expensive so is better to branch.
       *  Additionally not looking at values for nulls allows us not reset the values.
       */
      if (byteLengths[i] == -1) {
        continue;
      }
      if (bytesHash == null) {
        bytesHash = HashContext.getBytesHash(hashCtx);
        bytesHash.start(hash);
      }
      bytesHash.add(byteValues[i], byteStarts[i], byteLengths[i]);
    }
    if (bytesHash != null) {
      hash = bytesHash.end();
    }
    this.hashcode = hash;
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperGeneral) {
      VectorHashKeyWrapperGeneral keyThat = (VectorHashKeyWrapperGeneral)that;
      // not comparing hashCtx - irrelevant
      return hashcode == keyThat.hashcode &&
          Arrays.equals(longValues, keyThat.longValues) &&
          Arrays.equals(doubleValues, keyThat.doubleValues) &&
          Arrays.equals(decimalValues,  keyThat.decimalValues) &&
          Arrays.equals(timestampValues,  keyThat.timestampValues) &&
          Arrays.equals(intervalDayTimeValues,  keyThat.intervalDayTimeValues) &&
          Arrays.equals(isNull, keyThat.isNull) &&
          byteValues.length == keyThat.byteValues.length &&
          (0 == byteValues.length || bytesEquals(keyThat));
    }
    return false;
  }

  private boolean bytesEquals(VectorHashKeyWrapperGeneral keyThat) {
    //By the time we enter here the byteValues.lentgh and isNull must have already been compared
    for (int i = 0; i < byteValues.length; ++i) {
      // the byte comparison is potentially expensive so is better to branch on null
      if (byteLengths[i] != -1) {
        if (!StringExpr.equal(
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
    VectorHashKeyWrapperGeneral clone = new VectorHashKeyWrapperGeneral();
    duplicateTo(clone);
    return clone;
  }

  private void duplicateTo(VectorHashKeyWrapperGeneral clone) {
    clone.hashCtx = hashCtx;
    clone.keyCount = keyCount;
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
        if (byteLengths[i] != -1) {
          clone.byteValues[i] = Arrays.copyOfRange(byteValues[i],
              byteStarts[i], byteStarts[i] + byteLengths[i]);
        }
      }
    } else {
      clone.byteValues = EMPTY_BYTES_ARRAY;
      clone.byteStarts = EMPTY_INT_ARRAY;
      clone.byteLengths = EMPTY_INT_ARRAY;
    }
    if (timestampValues.length > 0) {
      clone.timestampValues = new Timestamp[timestampValues.length];
      for(int i = 0; i < timestampValues.length; ++i) {
        clone.timestampValues[i] = (Timestamp) timestampValues[i].clone();
      }
    } else {
      clone.timestampValues = EMPTY_TIMESTAMP_ARRAY;
    }
    if (intervalDayTimeValues.length > 0) {
      clone.intervalDayTimeValues = new HiveIntervalDayTime[intervalDayTimeValues.length];
      for(int i = 0; i < intervalDayTimeValues.length; ++i) {
        clone.intervalDayTimeValues[i] = (HiveIntervalDayTime) intervalDayTimeValues[i].clone();
      }
    } else {
      clone.intervalDayTimeValues = EMPTY_INTERVAL_DAY_TIME_ARRAY;
    }

    clone.hashcode = hashcode;
    assert clone.equals(this);
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    isNull[keyIndex] = false;
    longValues[index] = v;
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    longValues[index] = v;
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    isNull[keyIndex] = true;
    longValues[index] = 0; // assign 0 to simplify hashcode
  }

  @Override
  public void assignDouble(int index, double d) {
    doubleValues[index] = d;
  }

  @Override
  public void assignNullDouble(int keyIndex, int index) {
    isNull[keyIndex] = true;
    doubleValues[index] = 0; // assign 0 to simplify hashcode
  }

  @Override
  public void assignString(int index, byte[] bytes, int start, int length) {
    Preconditions.checkState(bytes != null);
    byteValues[index] = bytes;
    byteStarts[index] = start;
    byteLengths[index] = length;
  }

  @Override
  public void assignNullString(int keyIndex, int index) {
    isNull[keyIndex] = true;
    byteValues[index] = null;
    byteStarts[index] = 0;
    // We need some value that indicates NULL.
    byteLengths[index] = -1;
  }

  @Override
  public void assignDecimal(int index, HiveDecimalWritable value) {
    decimalValues[index].set(value);
  }

  @Override
  public void assignNullDecimal(int keyIndex, int index) {
    isNull[keyIndex] = true;
    decimalValues[index].set(HiveDecimal.ZERO); // assign 0 to simplify hashcode
  }

  @Override
  public void assignTimestamp(int index, Timestamp value) {
    // Do not assign the input value object to the timestampValues array element.
    // Always copy value using set* methods.
    timestampValues[index].setTime(value.getTime());
    timestampValues[index].setNanos(value.getNanos());
  }

  @Override
  public void assignTimestamp(int index, TimestampColumnVector colVector, int elementNum) {
    colVector.timestampUpdate(timestampValues[index], elementNum);
  }

  @Override
  public void assignNullTimestamp(int keyIndex, int index) {
    isNull[keyIndex] = true;
    // assign 0 to simplify hashcode
    timestampValues[index].setTime(ZERO_TIMESTAMP.getTime());
    timestampValues[index].setNanos(ZERO_TIMESTAMP.getNanos());
  }

  @Override
  public void assignIntervalDayTime(int index, HiveIntervalDayTime value) {
    intervalDayTimeValues[index].set(value);
  }

  @Override
  public void assignIntervalDayTime(int index, IntervalDayTimeColumnVector colVector, int elementNum) {
    intervalDayTimeValues[index].set(colVector.asScratchIntervalDayTime(elementNum));
  }

  @Override
  public void assignNullIntervalDayTime(int keyIndex, int index) {
    isNull[keyIndex] = true;
    intervalDayTimeValues[index].set(ZERO_INTERVALDAYTIME); // assign 0 to simplify hashcode
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    StringBuilder sb = new StringBuilder();
    boolean isFirstKey = true;

    if (longValues.length > 0) {
      isFirstKey = false;
      sb.append("longs ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.longIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.longIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(longValues[i]);
          PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) columnSetInfo.typeInfos[keyIndex];
          // FUTURE: Add INTERVAL_YEAR_MONTH, etc, as desired.
          switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case DATE:
            {
              Date dt = new Date(0);
              dt.setTime(DateWritableV2.daysToMillis((int) longValues[i]));
              sb.append(" date ");
              sb.append(dt.toString());
            }
            break;
          default:
            // Add nothing more.
            break;
          }
        }
      }
    }
    if (doubleValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("doubles ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.doubleIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.doubleIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(doubleValues[i]);
        }
      }
    }
    if (byteValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.stringIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.stringIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(byteLengths[i]);
        }
      }
    }
    if (decimalValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = true;
      } else {
        sb.append(", ");
      }
      sb.append("decimals ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.decimalIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.decimalIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(decimalValues[i]);
        }
      }
    }
    if (timestampValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("timestamps ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.timestampIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.timestampIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(timestampValues[i]);
        }
      }
    }
    if (intervalDayTimeValues.length > 0) {
      if (isFirstKey) {
        isFirstKey = false;
      } else {
        sb.append(", ");
      }
      sb.append("interval day times ");
      boolean isFirstValue = true;
      for (int i = 0; i < columnSetInfo.intervalDayTimeIndices.length; i++) {
        if (isFirstValue) {
          isFirstValue = false;
        } else {
          sb.append(", ");
        }
        int keyIndex = columnSetInfo.intervalDayTimeIndices[i];
        if (isNull[keyIndex]) {
          sb.append("null");
        } else {
          sb.append(intervalDayTimeValues[i]);
        }
      }
    }

    return sb.toString();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    boolean isFirst = true;
    if (longValues.length > 0) {
      isFirst = false;
      sb.append("longs ");
      sb.append(Arrays.toString(longValues));
    }
    if (doubleValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("doubles ");
      sb.append(Arrays.toString(doubleValues));
    }
    if (byteValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("byte lengths ");
      sb.append(Arrays.toString(byteLengths));
    }
    if (decimalValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("decimals ");
      sb.append(Arrays.toString(decimalValues));
    }
    if (timestampValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("timestamps ");
      sb.append(Arrays.toString(timestampValues));
    }
    if (intervalDayTimeValues.length > 0) {
      if (isFirst) {
        isFirst = false;
      } else {
        sb.append(", ");
      }
      sb.append("interval day times ");
      sb.append(Arrays.toString(intervalDayTimeValues));
    }

    if (isFirst) {
      isFirst = false;
    } else {
      sb.append(", ");
    }
    sb.append("nulls ");
    sb.append(Arrays.toString(isNull));

    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    return longValues[i];
  }

  @Override
  public double getDoubleValue(int i) {
    return doubleValues[i];
  }

  @Override
  public byte[] getBytes(int i) {
    return byteValues[i];
  }

  @Override
  public int getByteStart(int i) {
    return byteStarts[i];
  }

  @Override
  public int getByteLength(int i) {
    return byteLengths[i];
  }

  @Override
  public HiveDecimalWritable getDecimal(int i) {
    return decimalValues[i];
  }

  @Override
  public Timestamp getTimestamp(int i) {
    return timestampValues[i];
  }

  @Override
  public HiveIntervalDayTime getIntervalDayTime(int i) {
    return intervalDayTimeValues[i];
  }

  @Override
  public int getVariableSize() {
    int variableSize = 0;
    for (int i=0; i<byteLengths.length; ++i) {
      JavaDataModel model = JavaDataModel.get();
      variableSize += model.lengthForByteArrayOfSize(byteLengths[i]);
    }
    return variableSize;
  }

  @Override
  public void clearIsNull() {
    Arrays.fill(isNull, false);
  }

  @Override
  public void setNull() {
    Arrays.fill(isNull, true);
  }

  @Override
  public boolean isNull(int keyIndex) {
    return isNull[keyIndex];
  }
}
