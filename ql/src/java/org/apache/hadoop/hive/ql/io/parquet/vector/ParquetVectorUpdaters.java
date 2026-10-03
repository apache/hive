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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * The updaters that read PLAIN pages in bulk, one per family of Hive vector, for the pairings of Parquet and Hive type
 * that need no conversion per value. Each reads the non-NULL values densely, then spreads them over the NULL rows; the
 * dictionary half is the vector kind's.
 */
final class ParquetVectorUpdaters {

  private ParquetVectorUpdaters() {
  }

  /** INT32 into INT, BIGINT, INTERVAL_YEAR_MONTH or DATE; INT64 into BIGINT or INTERVAL_YEAR_MONTH; BOOLEAN. */
  static final class LongUpdater extends DictionaryValues.Longs implements ParquetVectorUpdater {
    private final PrimitiveTypeName physical;
    private final boolean prolepticDates;

    LongUpdater(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType,
        boolean skipProlepticConversion) {
      super(dictionary, type, hiveType, skipProlepticConversion);
      this.physical = type.getPrimitiveTypeName();
      this.prolepticDates = !skipProlepticConversion
          && ((PrimitiveTypeInfo) hiveType).getPrimitiveCategory() == PrimitiveCategory.DATE;
    }

    @Override
    public void readValues(int total, int nonNull, int offset, ColumnVector column,
        VectorizedPlainValuesReader values) {
      long[] v = ((LongColumnVector) column).vector;
      switch (physical) {
      case INT32 -> values.readIntegers(nonNull, v, offset);
      case INT64 -> values.readLongs(nonNull, v, offset);
      default -> values.readBooleans(nonNull, v, offset);
      }
      for (int i = offset; prolepticDates && i < offset + nonNull; i++) {
        v[i] = CalendarUtils.convertDateToProleptic((int) v[i]);
      }
      spread(v, column.isNull, total, nonNull, offset);
    }
  }

  /** FLOAT into FLOAT, DOUBLE into DOUBLE. */
  static final class DoubleUpdater extends DictionaryValues.Doubles implements ParquetVectorUpdater {
    private final boolean isFloat;

    DoubleUpdater(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType);
      this.isFloat = type.getPrimitiveTypeName() == PrimitiveTypeName.FLOAT;
    }

    @Override
    public void readValues(int total, int nonNull, int offset, ColumnVector column,
        VectorizedPlainValuesReader values) {
      double[] v = ((DoubleColumnVector) column).vector;
      if (isFloat) {
        values.readFloats(nonNull, v, offset);
      } else {
        values.readDoubles(nonNull, v, offset);
      }
      spread(v, column.isNull, total, nonNull, offset);
    }
  }

  /**
   * DECIMAL into decimal64 at the file scale: the unscaled INT32, INT64 or big-endian FIXED_LEN_BYTE_ARRAY values; one
   * beyond the Hive precision is NULL with value 0.
   */
  static final class Decimal64Updater extends DictionaryValues.Decimal64s implements ParquetVectorUpdater {
    private final PrimitiveTypeName physical;
    private final int width;
    private final long absMax;

    Decimal64Updater(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType);
      this.physical = type.getPrimitiveTypeName();
      this.width = type.getTypeLength();
      this.absMax = HiveDecimalWritable.getDecimal64AbsMax(((DecimalTypeInfo) hiveType).getPrecision());
    }

    @Override
    public void readValues(int total, int nonNull, int offset, ColumnVector column,
        VectorizedPlainValuesReader values) {
      long[] v = ((LongColumnVector) column).vector;
      switch (physical) {
      case INT32 -> values.readIntegers(nonNull, v, offset);
      case INT64 -> values.readLongs(nonNull, v, offset);
      default -> values.readBigEndianLongs(nonNull, width, v, offset);
      }
      // The range check rides on the spread, which visits every value anyway.
      boolean outOfRange = false;
      if (nonNull == total) {
        for (int i = offset; i < offset + total; i++) {
          outOfRange |= (v[i] < -absMax) | (v[i] > absMax);
        }
      } else {
        boolean[] isNull = column.isNull;
        for (int i = offset + total - 1, j = offset + nonNull - 1; i >= offset; i--) {
          if (!isNull[i]) {
            long x = v[j--];
            v[i] = x;
            outOfRange |= (x < -absMax) | (x > absMax);
          }
        }
      }
      if (outOfRange) {
        for (int i = offset; i < offset + total; i++) {
          if (!column.isNull[i] && (v[i] < -absMax || v[i] > absMax)) {
            v[i] = 0;
            PerValueUpdater.setNullValue(column, i);
          }
        }
      }
    }
  }

  /** BINARY into STRING, CHAR, VARCHAR or BINARY; only a UTF8 column truncates to the CHAR or VARCHAR length. */
  static final class BinaryUpdater extends DictionaryValues.Bytes implements ParquetVectorUpdater {
    private final int maxLength;

    BinaryUpdater(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType, int maxLength) {
      super(dictionary, type, hiveType);
      this.maxLength = maxLength;
    }

    @Override
    public void readValues(int total, int nonNull, int offset, ColumnVector column,
        VectorizedPlainValuesReader values) {
      BytesColumnVector c = (BytesColumnVector) column;
      values.readBinary(nonNull, c, offset, maxLength);
      for (int i = offset + total - 1, j = offset + nonNull - 1; nonNull < total && i >= offset; i--) {
        if (!c.isNull[i]) {
          c.vector[i] = c.vector[j];
          c.start[i] = c.start[j];
          c.length[i] = c.length[j--];
        }
      }
    }
  }

  /** Moves the dense values {@code v[offset .. offset + nonNull)} out to the non-NULL rows, from the last one. */
  private static void spread(long[] v, boolean[] isNull, int total, int nonNull, int offset) {
    if (nonNull == total) {
      return;
    }
    for (int i = offset + total - 1, j = offset + nonNull - 1; i >= offset; i--) {
      if (!isNull[i]) {
        v[i] = v[j--];
      }
    }
  }

  private static void spread(double[] v, boolean[] isNull, int total, int nonNull, int offset) {
    if (nonNull == total) {
      return;
    }
    for (int i = offset + total - 1, j = offset + nonNull - 1; i >= offset; i--) {
      if (!isNull[i]) {
        v[i] = v[j--];
      }
    }
  }
}
