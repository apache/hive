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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

/**
 * VectorExpressionWritableFactory helper class for generating VectorExpressionWritable objects.
 */
public final class VectorExpressionWriterFactory {

  /**
   * VectorExpressionWriter base implementation, to be specialized for Long/Double/Bytes columns
   */
  private static abstract class VectorExpressionWriterBase implements VectorExpressionWriter {

    protected ObjectInspector objectInspector;

    /**
     * The object inspector associated with this expression. This is created from the expression
     * NodeDesc (compile metadata) not from the VectorColumn info and thus preserves the type info
     * lost by the vectorization process.
     */
    public ObjectInspector getObjectInspector() {
      return objectInspector;
    }

    public VectorExpressionWriter init(ObjectInspector objectInspector) throws HiveException {
      this.objectInspector = objectInspector;
      return this;
    }

    /**
     * The base implementation must be overridden by the Long specialization
     */
    @Override
    public Object writeValue(long value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Long specialization
     */
    public Object setValue(Object field, long value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Double specialization
     */
    @Override
    public Object writeValue(double value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Double specialization
     */
    public Object setValue(Object field, double value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Bytes specialization
     */
    @Override
    public Object writeValue(byte[] value, int start, int length) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Bytes specialization
     */
    public Object setValue(Object field, byte[] value, int start, int length) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Decimal specialization
     */
    @Override
    public Object writeValue(HiveDecimal value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Decimal specialization
     */
    @Override
    public Object writeValue(HiveDecimalWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Decimal specialization
     */
    public Object setValue(Object field, HiveDecimalWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Decimal specialization
     */
    public Object setValue(Object field, HiveDecimal value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Timestamp specialization
     */
    @Override
    public Object writeValue(Timestamp value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Timestamp specialization
     */
    @Override
    public Object writeValue(TimestampWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Timestamp specialization
     */
    public Object setValue(Object field, TimestampWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the Timestamp specialization
     */
    public Object setValue(Object field, Timestamp value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the HiveIntervalDayTime specialization
     */
    @Override
    public Object writeValue(HiveIntervalDayTimeWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the HiveIntervalDayTime specialization
     */
    @Override
    public Object writeValue(HiveIntervalDayTime value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the HiveIntervalDayTime specialization
     */
    public Object setValue(Object field, HiveIntervalDayTimeWritable value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }

    /**
     * The base implementation must be overridden by the HiveIntervalDayTime specialization
     */
    public Object setValue(Object field, HiveIntervalDayTime value) throws HiveException {
      throw new HiveException("Internal error: should not reach here");
    }
  }

  /**
   * Specialized writer for LongVectorColumn expressions. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterLong
    extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      LongColumnVector lcv = (LongColumnVector) column;
      if (lcv.noNulls && !lcv.isRepeating) {
        return writeValue(lcv.vector[row]);
      } else if (lcv.noNulls && lcv.isRepeating) {
        return writeValue(lcv.vector[0]);
      } else if (!lcv.noNulls && !lcv.isRepeating && !lcv.isNull[row]) {
        return writeValue(lcv.vector[row]);
      } else if (!lcv.noNulls && !lcv.isRepeating && lcv.isNull[row]) {
        return null;
      } else if (!lcv.noNulls && lcv.isRepeating && !lcv.isNull[0]) {
        return writeValue(lcv.vector[0]);
      } else if (!lcv.noNulls && lcv.isRepeating && lcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, lcv.noNulls, lcv.isRepeating, lcv.isNull[row], lcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      LongColumnVector lcv = (LongColumnVector) column;
      if (lcv.noNulls && !lcv.isRepeating) {
        return setValue(field, lcv.vector[row]);
      } else if (lcv.noNulls && lcv.isRepeating) {
        return setValue(field, lcv.vector[0]);
      } else if (!lcv.noNulls && !lcv.isRepeating && !lcv.isNull[row]) {
        return setValue(field, lcv.vector[row]);
      } else if (!lcv.noNulls && !lcv.isRepeating && lcv.isNull[row]) {
        return null;
      } else if (!lcv.noNulls && lcv.isRepeating && !lcv.isNull[0]) {
        return setValue(field, lcv.vector[0]);
      } else if (!lcv.noNulls && lcv.isRepeating && lcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, lcv.noNulls, lcv.isRepeating, lcv.isNull[row], lcv.isNull[0]));
    }
  }

  /**
   * Specialized writer for DoubleColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterDouble extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      DoubleColumnVector dcv = (DoubleColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return writeValue(dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return writeValue(dcv.vector[row]);
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      DoubleColumnVector dcv = (DoubleColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return setValue(field, dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return setValue(field, dcv.vector[row]);
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }
   }

  /**
   * Specialized writer for BytesColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterBytes extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      BytesColumnVector bcv = (BytesColumnVector) column;
      if (bcv.noNulls && !bcv.isRepeating) {
        return writeValue(bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (bcv.noNulls && bcv.isRepeating) {
        return writeValue(bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && !bcv.isRepeating && !bcv.isNull[row]) {
        return writeValue(bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (!bcv.noNulls && !bcv.isRepeating && bcv.isNull[row]) {
        return null;
      } else if (!bcv.noNulls && bcv.isRepeating && !bcv.isNull[0]) {
        return writeValue(bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && bcv.isRepeating && bcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, bcv.noNulls, bcv.isRepeating, bcv.isNull[row], bcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      BytesColumnVector bcv = (BytesColumnVector) column;
      if (bcv.noNulls && !bcv.isRepeating) {
        return setValue(field, bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (bcv.noNulls && bcv.isRepeating) {
        return setValue(field, bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && !bcv.isRepeating && !bcv.isNull[row]) {
        return setValue(field, bcv.vector[row], bcv.start[row], bcv.length[row]);
      } else if (!bcv.noNulls && !bcv.isRepeating && bcv.isNull[row]) {
        return null;
      } else if (!bcv.noNulls && bcv.isRepeating && !bcv.isNull[0]) {
        return setValue(field, bcv.vector[0], bcv.start[0], bcv.length[0]);
      } else if (!bcv.noNulls && bcv.isRepeating && bcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
        String.format(
          "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
          row, bcv.noNulls, bcv.isRepeating, bcv.isNull[row], bcv.isNull[0]));
    }
  }


  /**
   * Specialized writer for DecimalColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterDecimal extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      DecimalColumnVector dcv = (DecimalColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return writeValue(dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return writeValue(dcv.vector[row]);
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return writeValue(dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      DecimalColumnVector dcv = (DecimalColumnVector) column;
      if (dcv.noNulls && !dcv.isRepeating) {
        return setValue(field, dcv.vector[row]);
      } else if (dcv.noNulls && dcv.isRepeating) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return setValue(field, dcv.vector[row]);
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return setValue(field, dcv.vector[0]);
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }
  }

  /**
   * Specialized writer for TimestampColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterTimestamp extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      TimestampColumnVector dcv = (TimestampColumnVector) column;
      TimestampWritable timestampWritable = (TimestampWritable) dcv.getScratchWritable();
      if (timestampWritable == null) {
        timestampWritable = new TimestampWritable();
        dcv.setScratchWritable(timestampWritable);
      }
      if (dcv.noNulls && !dcv.isRepeating) {
        return writeValue(TimestampUtils.timestampColumnVectorWritable(dcv, row, timestampWritable));
      } else if (dcv.noNulls && dcv.isRepeating) {
        return writeValue(TimestampUtils.timestampColumnVectorWritable(dcv, 0, timestampWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return writeValue(TimestampUtils.timestampColumnVectorWritable(dcv, row, timestampWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return writeValue(TimestampUtils.timestampColumnVectorWritable(dcv, 0, timestampWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      TimestampColumnVector dcv = (TimestampColumnVector) column;
      TimestampWritable timestampWritable = (TimestampWritable) dcv.getScratchWritable();
      if (timestampWritable == null) {
        timestampWritable = new TimestampWritable();
        dcv.setScratchWritable(timestampWritable);
      }
      if (dcv.noNulls && !dcv.isRepeating) {
        return setValue(field, TimestampUtils.timestampColumnVectorWritable(dcv, row, timestampWritable));
      } else if (dcv.noNulls && dcv.isRepeating) {
        return setValue(field, TimestampUtils.timestampColumnVectorWritable(dcv, 0, timestampWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return setValue(field, TimestampUtils.timestampColumnVectorWritable(dcv, row, timestampWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return setValue(field, TimestampUtils.timestampColumnVectorWritable(dcv, 0, timestampWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }
  }

  /**
   * Specialized writer for IntervalDayTimeColumnVector. Will throw cast exception
   * if the wrong vector column is used.
   */
  private static abstract class VectorExpressionWriterIntervalDayTime extends VectorExpressionWriterBase {
    @Override
    public Object writeValue(ColumnVector column, int row) throws HiveException {
      IntervalDayTimeColumnVector dcv = (IntervalDayTimeColumnVector) column;
      HiveIntervalDayTimeWritable intervalDayTimeWritable = (HiveIntervalDayTimeWritable) dcv.getScratchWritable();
      if (intervalDayTimeWritable == null) {
        intervalDayTimeWritable = new HiveIntervalDayTimeWritable();
        dcv.setScratchWritable(intervalDayTimeWritable);
      }
      if (dcv.noNulls && !dcv.isRepeating) {
        return writeValue(TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, row, intervalDayTimeWritable));
      } else if (dcv.noNulls && dcv.isRepeating) {
        return writeValue(TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, 0, intervalDayTimeWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return writeValue(TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, row, intervalDayTimeWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return writeValue(TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, 0, intervalDayTimeWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }

    @Override
    public Object setValue(Object field, ColumnVector column, int row) throws HiveException {
      IntervalDayTimeColumnVector dcv = (IntervalDayTimeColumnVector) column;
      HiveIntervalDayTimeWritable intervalDayTimeWritable = (HiveIntervalDayTimeWritable) dcv.getScratchWritable();
      if (intervalDayTimeWritable == null) {
        intervalDayTimeWritable = new HiveIntervalDayTimeWritable();
        dcv.setScratchWritable(intervalDayTimeWritable);
      }
      if (dcv.noNulls && !dcv.isRepeating) {
        return setValue(field, TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, row, intervalDayTimeWritable));
      } else if (dcv.noNulls && dcv.isRepeating) {
        return setValue(field, TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, 0, intervalDayTimeWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && !dcv.isNull[row]) {
        return setValue(field, TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, row, intervalDayTimeWritable));
      } else if (!dcv.noNulls && !dcv.isRepeating && dcv.isNull[row]) {
        return null;
      } else if (!dcv.noNulls && dcv.isRepeating && !dcv.isNull[0]) {
        return setValue(field, TimestampUtils.intervalDayTimeColumnVectorWritable(dcv, 0, intervalDayTimeWritable));
      } else if (!dcv.noNulls && dcv.isRepeating && dcv.isNull[0]) {
        return null;
      }
      throw new HiveException(
          String.format(
              "Incorrect null/repeating: row:%d noNulls:%b isRepeating:%b isNull[row]:%b isNull[0]:%b",
              row, dcv.noNulls, dcv.isRepeating, dcv.isNull[row], dcv.isNull[0]));
    }
  }

    /**
     * Compiles the appropriate vector expression writer based on an expression info (ExprNodeDesc)
     */
    public static VectorExpressionWriter genVectorExpressionWritable(ExprNodeDesc nodeDesc)
      throws HiveException {
      ObjectInspector objectInspector = nodeDesc.getWritableObjectInspector();
      if (null == objectInspector) {
        objectInspector = TypeInfoUtils
            .getStandardWritableObjectInspectorFromTypeInfo(nodeDesc.getTypeInfo());
      }
      if (null == objectInspector) {
        throw new HiveException(String.format(
            "Failed to initialize VectorExpressionWriter for expr: %s",
            nodeDesc.getExprString()));
      }
      return genVectorExpressionWritable(objectInspector);
    }

    /**
     * Compiles the appropriate vector expression writer based on an expression info (ExprNodeDesc)
     */
    public static VectorExpressionWriter genVectorExpressionWritable(
        ObjectInspector fieldObjInspector) throws HiveException {

      switch (fieldObjInspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) fieldObjInspector).getPrimitiveCategory()) {
          case FLOAT:
            return genVectorExpressionWritableFloat(
                (SettableFloatObjectInspector) fieldObjInspector);
          case DOUBLE:
            return genVectorExpressionWritableDouble(
                (SettableDoubleObjectInspector) fieldObjInspector);
          case BOOLEAN:
            return genVectorExpressionWritableBoolean(
                (SettableBooleanObjectInspector) fieldObjInspector);
          case BYTE:
            return genVectorExpressionWritableByte(
                (SettableByteObjectInspector) fieldObjInspector);
          case SHORT:
            return genVectorExpressionWritableShort(
                (SettableShortObjectInspector) fieldObjInspector);
          case INT:
            return genVectorExpressionWritableInt(
                (SettableIntObjectInspector) fieldObjInspector);
          case LONG:
            return genVectorExpressionWritableLong(
                (SettableLongObjectInspector) fieldObjInspector);
          case VOID:
              return genVectorExpressionWritableVoid(
                  (VoidObjectInspector) fieldObjInspector);
          case BINARY:
            return genVectorExpressionWritableBinary(
                (SettableBinaryObjectInspector) fieldObjInspector);
          case STRING:
            return genVectorExpressionWritableString(
                (SettableStringObjectInspector) fieldObjInspector);
          case CHAR:
              return genVectorExpressionWritableChar(
                  (SettableHiveCharObjectInspector) fieldObjInspector);
          case VARCHAR:
            return genVectorExpressionWritableVarchar(
                (SettableHiveVarcharObjectInspector) fieldObjInspector);
          case TIMESTAMP:
            return genVectorExpressionWritableTimestamp(
                (SettableTimestampObjectInspector) fieldObjInspector);
          case DATE:
            return genVectorExpressionWritableDate(
                (SettableDateObjectInspector) fieldObjInspector);
          case INTERVAL_YEAR_MONTH:
            return genVectorExpressionWritableIntervalYearMonth(
                (SettableHiveIntervalYearMonthObjectInspector) fieldObjInspector);
          case INTERVAL_DAY_TIME:
            return genVectorExpressionWritableIntervalDayTime(
                (SettableHiveIntervalDayTimeObjectInspector) fieldObjInspector);
          case DECIMAL:
            return genVectorExpressionWritableDecimal(
                (SettableHiveDecimalObjectInspector) fieldObjInspector);
          default:
            throw new IllegalArgumentException("Unknown primitive type: " +
              ((PrimitiveObjectInspector) fieldObjInspector).getPrimitiveCategory());
        }

      case STRUCT:
      case UNION:
      case MAP:
      case LIST:
        return genVectorExpressionWritableEmpty();
      default:
        throw new IllegalArgumentException("Unknown type " +
            fieldObjInspector.getCategory());
      }
  }

  /**
   * Compiles the appropriate vector expression writers based on a struct object
   * inspector.
   */
  public static VectorExpressionWriter[] genVectorStructExpressionWritables(
      StructObjectInspector oi) throws HiveException {
    VectorExpressionWriter[] writers = new VectorExpressionWriter[oi.getAllStructFieldRefs().size()];
    final List<? extends StructField> fields = oi.getAllStructFieldRefs();
    int i = 0;
    for(StructField field : fields) {
      writers[i++] = genVectorExpressionWritable(field.getFieldObjectInspector());
    }
    return writers;
  }

  private static VectorExpressionWriter genVectorExpressionWritableDecimal(
      SettableHiveDecimalObjectInspector fieldObjInspector) throws HiveException {

    return new VectorExpressionWriterDecimal() {
      private Object obj;

      public VectorExpressionWriter init(SettableHiveDecimalObjectInspector objInspector) throws HiveException {
        super.init(objInspector);
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(HiveDecimalWritable value) throws HiveException {
        return ((SettableHiveDecimalObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object writeValue(HiveDecimal value) throws HiveException {
        return ((SettableHiveDecimalObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object setValue(Object field, HiveDecimalWritable value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableHiveDecimalObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object setValue(Object field, HiveDecimal value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableHiveDecimalObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object setValue(Object field, TimestampWritable value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableTimestampObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object setValue(Object field, Timestamp value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableTimestampObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveDecimalObjectInspector) this.objectInspector).create(
            HiveDecimal.ZERO);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableDate(
      SettableDateObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Date dt;
      private Object obj;

      public VectorExpressionWriter init(SettableDateObjectInspector objInspector) throws HiveException {
        super.init(objInspector);
        dt = new Date(0);
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) {
        dt.setTime(DateWritable.daysToMillis((int) value));
        ((SettableDateObjectInspector) this.objectInspector).set(obj, dt);
        return obj;
      }

      @Override
      public Object setValue(Object field, long value) {
        if (null == field) {
          field = initValue(null);
        }
        dt.setTime(DateWritable.daysToMillis((int) value));
        ((SettableDateObjectInspector) this.objectInspector).set(field, dt);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableDateObjectInspector) this.objectInspector).create(new Date(0));
      }

    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableTimestamp(
      SettableTimestampObjectInspector fieldObjInspector) throws HiveException {

    return new VectorExpressionWriterTimestamp() {
      private Object obj;

      public VectorExpressionWriter init(SettableTimestampObjectInspector objInspector) throws HiveException {
        super.init(objInspector);
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(TimestampWritable value) throws HiveException {
        return ((SettableTimestampObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object writeValue(Timestamp value) throws HiveException {
        return ((SettableTimestampObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object writeValue(HiveIntervalDayTimeWritable value) throws HiveException {
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object writeValue(HiveIntervalDayTime value) throws HiveException {
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(obj, value);
      }

      @Override
      public Object setValue(Object field, TimestampWritable value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableTimestampObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object setValue(Object field, Timestamp value) {
        if (null == field) {
          field = initValue(null);
        }
        return ((SettableTimestampObjectInspector) this.objectInspector).set(field, value);
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableTimestampObjectInspector) this.objectInspector).create(new Timestamp(0));
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableIntervalYearMonth(
      SettableHiveIntervalYearMonthObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;
      private HiveIntervalYearMonth interval;

      public VectorExpressionWriter init(SettableHiveIntervalYearMonthObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        interval = new HiveIntervalYearMonth();
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) {
        interval.set((int) value);
        ((SettableHiveIntervalYearMonthObjectInspector) this.objectInspector).set(obj, interval);
        return obj;
      }

      @Override
      public Object setValue(Object field, long value) {
        if (null == field) {
          field = initValue(null);
        }
        interval.set((int) value);
        ((SettableHiveIntervalYearMonthObjectInspector) this.objectInspector).set(field, interval);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveIntervalYearMonthObjectInspector) this.objectInspector)
            .create(new HiveIntervalYearMonth());
      }
   }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableIntervalDayTime(
      SettableHiveIntervalDayTimeObjectInspector fieldObjInspector) throws HiveException {

    return new VectorExpressionWriterIntervalDayTime() {
      private Object obj;
      private HiveIntervalDayTime interval;

      public VectorExpressionWriter init(SettableHiveIntervalDayTimeObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        interval = new HiveIntervalDayTime();
        obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(HiveIntervalDayTimeWritable value) throws HiveException {
        interval.set(value.getHiveIntervalDayTime());
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(obj, interval);
      }

      @Override
      public Object writeValue(HiveIntervalDayTime value) throws HiveException {
        interval.set(value);
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(obj, interval);
      }

      @Override
      public Object setValue(Object field, HiveIntervalDayTimeWritable value) {
        if (null == field) {
          field = initValue(null);
        }
        interval.set(value.getHiveIntervalDayTime());
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(field, interval);
      }

      @Override
      public Object setValue(Object field, HiveIntervalDayTime value) {
        if (null == field) {
          field = initValue(null);
        }
        interval.set(value);
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector).set(field, interval);
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveIntervalDayTimeObjectInspector) this.objectInspector)
            .create(new HiveIntervalDayTime());
      }
   }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableChar(
        SettableHiveCharObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private Text text;
      
      public VectorExpressionWriter init(SettableHiveCharObjectInspector objInspector) 
          throws HiveException {
        super.init(objInspector);
        this.text = new Text();
        this.obj = initValue(null);
        return this;
      }
      
      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        text.set(value, start, length);
        ((SettableHiveCharObjectInspector) this.objectInspector).set(this.obj, text.toString());
        return this.obj;
      }

      @Override
      public Object setValue(Object field, byte[] value, int start, int length) 
          throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        text.set(value, start, length);
        ((SettableHiveCharObjectInspector) this.objectInspector).set(field, text.toString());
        return field;
      }
      
      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveCharObjectInspector) this.objectInspector)
            .create(new HiveChar(StringUtils.EMPTY, -1));
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableVarchar(
        SettableHiveVarcharObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private Text text;

      public VectorExpressionWriter init(SettableHiveVarcharObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.text = new Text();
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        text.set(value, start, length);
        ((SettableHiveVarcharObjectInspector) this.objectInspector).set(this.obj, text.toString());
        return this.obj;
      }

      @Override
      public Object setValue(Object field, byte[] value, int start, int length)
          throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        text.set(value, start, length);
        ((SettableHiveVarcharObjectInspector) this.objectInspector).set(field, text.toString());
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableHiveVarcharObjectInspector) this.objectInspector)
            .create(new HiveVarchar(StringUtils.EMPTY, -1));
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableString(
      SettableStringObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private Text text;

      public VectorExpressionWriter init(SettableStringObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.text = new Text();
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        this.text.set(value, start, length);
        ((SettableStringObjectInspector) this.objectInspector).set(this.obj, this.text);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, byte[] value, int start, int length)
          throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        this.text.set(value, start, length);
        ((SettableStringObjectInspector) this.objectInspector).set(field, this.text);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableStringObjectInspector) this.objectInspector).create(StringUtils.EMPTY);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableBinary(
      SettableBinaryObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterBytes() {
      private Object obj;
      private byte[] bytes;

      public VectorExpressionWriter init(SettableBinaryObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.bytes = ArrayUtils.EMPTY_BYTE_ARRAY;
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(byte[] value, int start, int length) throws HiveException {
        bytes = Arrays.copyOfRange(value, start, start + length);
        ((SettableBinaryObjectInspector) this.objectInspector).set(this.obj, bytes);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, byte[] value, int start, int length) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        bytes = Arrays.copyOfRange(value, start, start + length);
        ((SettableBinaryObjectInspector) this.objectInspector).set(field, bytes);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableBinaryObjectInspector) this.objectInspector)
            .create(ArrayUtils.EMPTY_BYTE_ARRAY);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableLong(
      SettableLongObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(SettableLongObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableLongObjectInspector) this.objectInspector).set(this.obj, value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableLongObjectInspector) this.objectInspector).set(field, value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableLongObjectInspector) this.objectInspector)
            .create(0L);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableVoid(
    VoidObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(VoidObjectInspector objInspector) throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((VoidObjectInspector) this.objectInspector).copyObject(null);
      }
    }.init(fieldObjInspector);
  }


  private static VectorExpressionWriter genVectorExpressionWritableInt(
      SettableIntObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(SettableIntObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableIntObjectInspector) this.objectInspector).set(this.obj, (int) value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableIntObjectInspector) this.objectInspector).set(field, (int) value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableIntObjectInspector) this.objectInspector)
            .create(0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableShort(
      SettableShortObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(SettableShortObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableShortObjectInspector) this.objectInspector).set(this.obj, (short) value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableShortObjectInspector) this.objectInspector).set(field, (short) value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableShortObjectInspector) this.objectInspector)
            .create((short) 0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableByte(
      SettableByteObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(SettableByteObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableByteObjectInspector) this.objectInspector).set(this.obj, (byte) value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableByteObjectInspector) this.objectInspector).set(field, (byte) value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableByteObjectInspector) this.objectInspector)
            .create((byte) 0);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableBoolean(
      SettableBooleanObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterLong() {
      private Object obj;

      public VectorExpressionWriter init(SettableBooleanObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(long value) throws HiveException {
        ((SettableBooleanObjectInspector) this.objectInspector).set(this.obj,
            value == 0 ? false : true);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, long value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableBooleanObjectInspector) this.objectInspector).set(field,
            value == 0 ? false : true);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableBooleanObjectInspector) this.objectInspector)
            .create(false);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableDouble(
      SettableDoubleObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterDouble() {
      private Object obj;

      public VectorExpressionWriter init(SettableDoubleObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(double value) throws HiveException {
        ((SettableDoubleObjectInspector) this.objectInspector).set(this.obj, value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, double value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableDoubleObjectInspector) this.objectInspector).set(field, value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableDoubleObjectInspector) this.objectInspector)
            .create(0f);
      }
    }.init(fieldObjInspector);
  }

  private static VectorExpressionWriter genVectorExpressionWritableFloat(
      SettableFloatObjectInspector fieldObjInspector) throws HiveException {
    return new VectorExpressionWriterDouble() {
      private Object obj;

      public VectorExpressionWriter init(SettableFloatObjectInspector objInspector)
          throws HiveException {
        super.init(objInspector);
        this.obj = initValue(null);
        return this;
      }

      @Override
      public Object writeValue(double value) throws HiveException {
        ((SettableFloatObjectInspector) this.objectInspector).set(this.obj, (float) value);
        return this.obj;
      }

      @Override
      public Object setValue(Object field, double value) throws HiveException {
        if (null == field) {
          field = initValue(null);
        }
        ((SettableFloatObjectInspector) this.objectInspector).set(field, (float) value);
        return field;
      }

      @Override
      public Object initValue(Object ignored) {
        return ((SettableFloatObjectInspector) this.objectInspector)
            .create(0f);
      }
    }.init(fieldObjInspector);
  }

  // For complex types like STRUCT, MAP, etc we do not support, we need a writer that
  // does nothing.  We assume the Vectorizer class has not validated the query to actually
  // try and use the complex types.  They do show up in inputObjInspector[0] and need to be
  // ignored.
  private static VectorExpressionWriter genVectorExpressionWritableEmpty() {
    return new VectorExpressionWriterBase() {

      @Override
      public Object writeValue(ColumnVector column, int row)
          throws HiveException {
        return null;
      }

      @Override
      public Object setValue(Object row, ColumnVector column, int columnRow)
          throws HiveException {
        return null;
      }

      @Override
      public Object initValue(Object ost) throws HiveException {
        return null;
      }
    };
  }

  /**
   * Helper function to create an array of writers from a list of expression descriptors.
   */
  public static VectorExpressionWriter[] getExpressionWriters(List<ExprNodeDesc> nodesDesc)
      throws HiveException {
    VectorExpressionWriter[] writers = new VectorExpressionWriter[nodesDesc.size()];
    for(int i=0; i<writers.length; ++i) {
      ExprNodeDesc nodeDesc = nodesDesc.get(i);
      writers[i] = genVectorExpressionWritable(nodeDesc);
    }
    return writers;
  }

  /**
   * A poor man Java closure. Works around the problem of having to return multiple objects
   * from one function call.
   */
  public static interface SingleOIDClosure {
    void assign(VectorExpressionWriter[] writers, ObjectInspector objectInspector);
  }

  public static interface ListOIDClosure {
    void assign(VectorExpressionWriter[] writers, List<ObjectInspector> oids);
  }

  /**
   * Creates the value writers for a column vector expression list.
   * Creates an appropriate output object inspector.
   */
  public static void processVectorExpressions(
      List<ExprNodeDesc> nodesDesc,
      List<String> columnNames,
      SingleOIDClosure closure)
      throws HiveException {
    VectorExpressionWriter[] writers = getExpressionWriters(nodesDesc);
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>(writers.length);
    for(int i=0; i<writers.length; ++i) {
      oids.add(writers[i].getObjectInspector());
    }
    ObjectInspector objectInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(columnNames,oids);
    closure.assign(writers, objectInspector);
  }

  /**
   * Creates the value writers for a column vector expression list.
   * Creates an appropriate output object inspector.
   */
  public static void processVectorExpressions(
      List<ExprNodeDesc> nodesDesc,
      ListOIDClosure closure)
      throws HiveException {
    VectorExpressionWriter[] writers = getExpressionWriters(nodesDesc);
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>(writers.length);
    for(int i=0; i<writers.length; ++i) {
      oids.add(writers[i].getObjectInspector());
    }
    closure.assign(writers, oids);
  }

  /**
   * Creates the value writers for an struct object inspector.
   * Creates an appropriate output object inspector.
   */
  public static void processVectorInspector(
      StructObjectInspector structObjInspector,
      SingleOIDClosure closure)
      throws HiveException {
    List<? extends StructField> fields = structObjInspector.getAllStructFieldRefs();
    VectorExpressionWriter[] writers = new VectorExpressionWriter[fields.size()];
    List<ObjectInspector> oids = new ArrayList<ObjectInspector>(writers.length);
    ArrayList<String> columnNames = new ArrayList<String>();
    int i = 0;
    for(StructField field : fields) {
      ObjectInspector fieldObjInsp = field.getFieldObjectInspector();
      writers[i] = VectorExpressionWriterFactory.
                genVectorExpressionWritable(fieldObjInsp);
      columnNames.add(field.getFieldName());
      oids.add(writers[i].getObjectInspector());
      i++;
    }
    ObjectInspector objectInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(columnNames,oids);
    closure.assign(writers, objectInspector);
  }

  /**
   * Returns {@link VectorExpressionWriter} objects for the fields in the given
   * object inspector.
   *
   * @param objInspector
   * @return
   * @throws HiveException
   */
  public static VectorExpressionWriter[] getExpressionWriters(StructObjectInspector objInspector)
      throws HiveException {

    if (objInspector.isSettable()) {
      return getSettableExpressionWriters((SettableStructObjectInspector) objInspector);
    }

    List<? extends StructField> allFieldRefs = objInspector.getAllStructFieldRefs();

    VectorExpressionWriter[] expressionWriters = new VectorExpressionWriter[allFieldRefs.size()];

    for(int i=0; i<expressionWriters.length; ++i) {
      expressionWriters[i] = genVectorExpressionWritable(allFieldRefs.get(i).getFieldObjectInspector());
    }

    return expressionWriters;
  }

  public static VectorExpressionWriter[] getSettableExpressionWriters(
      SettableStructObjectInspector objInspector) throws HiveException {
    List<? extends StructField> fieldsRef = objInspector.getAllStructFieldRefs();
    VectorExpressionWriter[] writers = new VectorExpressionWriter[fieldsRef.size()];
    for(int i=0; i<writers.length; ++i) {
      StructField fieldRef = fieldsRef.get(i);
      VectorExpressionWriter baseWriter = genVectorExpressionWritable(
          fieldRef.getFieldObjectInspector());
      writers[i] = genVectorExpressionWritable(objInspector, fieldRef, baseWriter);
    }
    return writers;

  }

  /**
   * VectorExpressionWriterSetter helper for vector expression writers that use
   * settable ObjectInspector fields to assign the values.
   * This is used by the OrcStruct serialization (eg. CREATE TABLE ... AS ...)
   */
  private static class VectorExpressionWriterSetter extends VectorExpressionWriterBase {
    private SettableStructObjectInspector settableObjInspector;
    private StructField fieldRef;
    private VectorExpressionWriter baseWriter;

    public VectorExpressionWriterSetter init(
        SettableStructObjectInspector objInspector,
        StructField fieldRef,
        VectorExpressionWriter baseWriter) {
      this.fieldRef = fieldRef;
      this.settableObjInspector = objInspector;
      this.objectInspector = fieldRef.getFieldObjectInspector();
      this.baseWriter = baseWriter;
      return this;
    }

    @Override
    public Object writeValue(ColumnVector column, int row)
        throws HiveException {
      return baseWriter.writeValue(column, row);
    }

    @Override
    public Object setValue(Object row, ColumnVector column, int columnRow)
        throws HiveException {

      // NULLs are handled by each individual base writer setter
      // We could handle NULLs centrally here but that would result in spurious allocs

      Object fieldValue = this.settableObjInspector.getStructFieldData(row, fieldRef);
      fieldValue = baseWriter.setValue(fieldValue, column, columnRow);
      return this.settableObjInspector.setStructFieldData(row, fieldRef, fieldValue);
    }

    @Override
    public Object initValue(Object struct) throws HiveException {
      Object initValue = this.baseWriter.initValue(null);
      this.settableObjInspector.setStructFieldData(struct, fieldRef, initValue);
      return struct;
    }
  }

  private static VectorExpressionWriter genVectorExpressionWritable(
      SettableStructObjectInspector objInspector,
      StructField fieldRef,
      VectorExpressionWriter baseWriter) throws HiveException {
    return new VectorExpressionWriterSetter().init(objInspector, fieldRef, baseWriter);
  }
}
