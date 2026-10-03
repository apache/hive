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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Reads a column's values one at a time through the ParquetDataColumnReader of the page, which converts each value to
 * the Hive type; part of the code is referred from Apache Spark and Apache Parquet.
 *
 * The read calls correspond to the various hivetypes.  When the data was read, it would have been
 * checked for validity with respect to the hivetype.  The isValid call will return the result
 * of that check.  The readers will keep the value as returned by the reader when valid, and
 * set the value to null when it is invalid.
 */
final class PerValueUpdater implements ParquetVectorUpdater {

  private final ColumnDescriptor descriptor;
  private final PrimitiveType type;
  private final TypeInfo hiveType;
  private final DictionaryValues dictionaryValues;
  private final boolean skipTimestampConversion;
  private final ZoneId writerTimezone;
  private final boolean skipProlepticConversion;
  private final boolean legacyConversionEnabled;
  private final DecimalPrecisionScale decimals;
  private ParquetDataColumnReader dataColumn;

  PerValueUpdater(ColumnDescriptor descriptor, PrimitiveType type, TypeInfo hiveType,
      DictionaryValues dictionaryValues, boolean skipTimestampConversion, ZoneId writerTimezone,
      boolean skipProlepticConversion, boolean legacyConversionEnabled) {
    this.descriptor = descriptor;
    this.type = type;
    this.hiveType = hiveType;
    this.dictionaryValues = dictionaryValues;
    this.skipTimestampConversion = skipTimestampConversion;
    this.writerTimezone = writerTimezone;
    this.skipProlepticConversion = skipProlepticConversion;
    this.legacyConversionEnabled = legacyConversionEnabled;
    this.decimals = new DecimalPrecisionScale(type, hiveType);
  }

  @Override
  public void beginBatch(ColumnVector column) {
    dictionaryValues.beginBatch(column);
  }

  @Override
  public boolean readsPlainInBulk() {
    return false;
  }

  /** A new page that is not dictionary encoded, read through parquet-mr's {@code values}. */
  void startPage(ValuesReader values) throws IOException {
    dataColumn = ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, hiveType, values,
        skipTimestampConversion, writerTimezone, legacyConversionEnabled);
  }

  @Override
  public void readValues(int total, int nonNull, int offset, ColumnVector column, VectorizedPlainValuesReader values)
      throws IOException {
    readBatchHelper(total, column, hiveType, offset);
  }

  @Override
  public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
    dictionaryValues.decodeDictionaryIds(total, nonNull, offset, column, ids);
  }

  @Override
  public boolean isRepeating(ColumnVector column, int total, boolean dictionaryOnly, boolean sameIds) {
    return dictionaryValues.isRepeating(column, total, dictionaryOnly, sameIds);
  }

  private void readBatchHelper(
      int num,
      ColumnVector column,
      TypeInfo columnType,
      int rowId) throws IOException {
    PrimitiveTypeInfo primitiveColumnType = (PrimitiveTypeInfo) columnType;

    switch (primitiveColumnType.getPrimitiveCategory()) {
    case INT:
      readIntegers(num, (LongColumnVector) column, rowId);
      break;
    case BYTE:
      readTinyInts(num, (LongColumnVector) column, rowId);
      break;
    case SHORT:
      readSmallInts(num, (LongColumnVector) column, rowId);
      break;
    case DATE:
      readDate(num, (DateColumnVector) column, rowId);
      break;
    case INTERVAL_YEAR_MONTH:
    case LONG:
      readLongs(num, (LongColumnVector) column, rowId);
      break;
    case BOOLEAN:
      readBooleans(num, (LongColumnVector) column, rowId);
      break;
    case DOUBLE:
      readDoubles(num, (DoubleColumnVector) column, rowId);
      break;
    case BINARY:
      readBinaries(num, (BytesColumnVector) column, rowId);
      break;
    case STRING:
      readString(num, (BytesColumnVector) column, rowId);
      break;
    case VARCHAR:
      readVarchar(num, (BytesColumnVector) column, rowId);
      break;
    case CHAR:
      readChar(num, (BytesColumnVector) column, rowId);
      break;
    case FLOAT:
      readFloats(num, (DoubleColumnVector) column, rowId);
      break;
    case DECIMAL:
      if (column instanceof Decimal64ColumnVector) {
        readDecimal64(num, (Decimal64ColumnVector) column, rowId);
      } else {
        readDecimal(num, (DecimalColumnVector) column, rowId);
      }
      break;
    case TIMESTAMP:
      readTimestamp(num, (TimestampColumnVector) column, rowId);
      break;
    case INTERVAL_DAY_TIME:
    default:
      throw new IOException("Unsupported type: " + type);
    }
  }

  static void setNullValue(ColumnVector c, int rowId) {
    c.isNull[rowId] = true;
    c.isRepeating = false;
    c.noNulls = false;
  }

  private void readIntegers(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readInteger();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readSmallInts(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readSmallInt();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readTinyInts(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readTinyInt();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readDoubles(int total, DoubleColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readDouble();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readBooleans(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readBoolean() ? 1 : 0;
      }
      rowId++;
      left--;
    }
  }

  private void readLongs(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readLong();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readFloats(int total, DoubleColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = dataColumn.readFloat();
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readDecimal(int total, DecimalColumnVector c, int rowId) {
    byte[] decimalData;
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        decimalData = dataColumn.readDecimal();
        if (dataColumn.isValid()) {
          c.vector[rowId].set(decimalData, c.scale);
        } else {
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readString(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.setVal(rowId, dataColumn.readString());
      }
      rowId++;
      left--;
    }
  }

  private void readChar(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.setVal(rowId, dataColumn.readChar());
      }
      rowId++;
      left--;
    }
  }

  private void readVarchar(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.setVal(rowId, dataColumn.readVarchar());
      }
      rowId++;
      left--;
    }
  }

  private void readBinaries(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.setVal(rowId, dataColumn.readBytes());
      }
      rowId++;
      left--;
    }
  }

  private void readDate(int total, DateColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        c.vector[rowId] = skipProlepticConversion ?
            dataColumn.readLong() : CalendarUtils.convertDateToProleptic((int) dataColumn.readLong());
        if (!dataColumn.isValid()) {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      }
      rowId++;
      left--;
    }
  }

  private void readTimestamp(int total, TimestampColumnVector c, int rowId) throws IOException {
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        switch (descriptor.getType()) {
        //INT64 is not yet supported
        case INT96:
          c.set(rowId, dataColumn.readTimestamp().toSqlTimestamp());
          break;
        case INT64:
          c.set(rowId, dataColumn.readTimestamp().toSqlTimestamp());
          break;
        default:
          throw new IOException(
              "Unsupported parquet logical type: " + type.getLogicalTypeAnnotation().toString() + " for timestamp");
        }
      }
      rowId++;
      left--;
    }
  }

  /**
   * Decimal64 fast path: read the unscaled value straight into the long-backed vector instead of
   * materializing a HiveDecimal per row. Only for columns the vectorizer tagged DECIMAL_64
   * (precision <= 18); higher precision uses {@link #readDecimal}.
   */
  private void readDecimal64(int total, Decimal64ColumnVector c, int rowId) {
    boolean fast = dataColumn.isFastDecimal64();
    short valueScale = (short) decimals.getDecimalTypeInfo().getScale();
    int left = total;
    while (left > 0) {
      if (!c.isNull[rowId]) {
        DecimalPrecisionScale.setDecimal64Value(c, rowId, fast, dataColumn, -1, valueScale);
      }
      rowId++;
      left--;
    }
  }
}
