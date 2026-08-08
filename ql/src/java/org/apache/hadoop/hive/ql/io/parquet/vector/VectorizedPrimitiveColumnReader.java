/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.time.ZoneId;

/**
 * It's column level Parquet reader which is used to read a batch of records for a column,
 * part of the code is referred from Apache Spark and Apache Parquet.
 *
 * The read calls correspond to the various hivetypes.  When the data was read, it would have been
 * checked for validity with respect to the hivetype.  The isValid call will return the result
 * of that check.  The readers will keep the value as returned by the reader when valid, and
 * set the value to null when it is invalid.
 */
public class VectorizedPrimitiveColumnReader extends BaseVectorizedColumnReader {

  public VectorizedPrimitiveColumnReader(
      ColumnDescriptor descriptor,
      PageReader pageReader,
      boolean skipTimestampConversion,
      ZoneId writerTimezone,
      boolean skipProlepticConversion,
      boolean legacyConversionEnabled,
      Type type,
      TypeInfo hiveType)
      throws IOException {
    super(descriptor, pageReader, skipTimestampConversion, writerTimezone, skipProlepticConversion,
        legacyConversionEnabled, type, hiveType);
  }

  @Override
  public void readBatch(
      int total,
      ColumnVector column,
      TypeInfo columnType) throws IOException {
    this.currentDefLevels = new int[total];
    this.defLevelIndex = 0;
    int rowId = 0;
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }

      int num = Math.min(total, leftInPage);
      if (isCurrentPageDictionaryEncoded) {
        LongColumnVector dictionaryIds = new LongColumnVector();
        // Read and decode dictionary ids.
        readDictionaryIDs(num, dictionaryIds, rowId);
        decodeDictionaryIds(rowId, num, column, columnType, dictionaryIds);
      } else {
        // assign values in vector
        readBatchHelper(num, column, columnType, rowId);
      }
      rowId += num;
      total -= num;
    }
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
    case TIMESTAMPLOCALTZ:
      readTimestamp(num, (TimestampColumnVector) column, rowId);
      break;
    case INTERVAL_DAY_TIME:
    default:
      throw new IOException("Unsupported type: " + type);
    }
  }

  private static void setNullValue(ColumnVector c, int rowId) {
    c.isNull[rowId] = true;
    c.isRepeating = false;
    c.noNulls = false;
  }

  private void readDictionaryIDs(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readValueDictionaryId();
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readIntegers(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readInteger();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readSmallInts(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readSmallInt();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readTinyInts(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readTinyInt();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readDoubles(int total, DoubleColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readDouble();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readBooleans(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readBoolean() ? 1 : 0;
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readLongs(int total, LongColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readLong();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readFloats(int total, DoubleColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = dataColumn.readFloat();
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readDecimal(int total, DecimalColumnVector c, int rowId) {
    byte[] decimalData;
    fillDecimalPrecisionScale(c);

    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        decimalData = dataColumn.readDecimal();
        if (dataColumn.isValid()) {
          c.vector[rowId].set(decimalData, c.scale);
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readString(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.setVal(rowId, dataColumn.readString());
        c.isNull[rowId] = false;
        // TODO figure out a better way to set repeat for Binary type
        c.isRepeating = false;
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readChar(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.setVal(rowId, dataColumn.readChar());
        c.isNull[rowId] = false;
        // TODO figure out a better way to set repeat for Binary type
        c.isRepeating = false;
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readVarchar(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.setVal(rowId, dataColumn.readVarchar());
        c.isNull[rowId] = false;
        // TODO figure out a better way to set repeat for Binary type
        c.isRepeating = false;
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readBinaries(int total, BytesColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.setVal(rowId, dataColumn.readBytes());
        c.isNull[rowId] = false;
        // TODO figure out a better way to set repeat for Binary type
        c.isRepeating = false;
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readDate(int total, DateColumnVector c, int rowId) {
    c.setUsingProlepticCalendar(true);
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId] = skipProlepticConversion ?
            dataColumn.readLong() : CalendarUtils.convertDateToProleptic((int) dataColumn.readLong());
        if (dataColumn.isValid()) {
          c.isNull[rowId] = false;
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        } else {
          c.vector[rowId] = 0;
          setNullValue(c, rowId);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  private void readTimestamp(int total, TimestampColumnVector c, int rowId) throws IOException {
    c.setUsingProlepticCalendar(true);
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        switch (descriptor.getType()) {
        //INT64 is not yet supported
        case INT96:
        case INT64:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          Timestamp t = dataColumn.readTimestamp();
          if (dataColumn.isValid()) {
            c.set(rowId, t.toSqlTimestamp());
            c.isNull[rowId] = false;
            c.isRepeating =
                c.isRepeating && ((c.time[0] == c.time[rowId]) && (c.nanos[0] == c.nanos[rowId]));
          } else {
            setNullValue(c, rowId);
          }
          break;
        default:
          throw new IOException(
              "Unsupported parquet logical type: " + type.getLogicalTypeAnnotation().toString() + " for timestamp");
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(
      int rowId,
      int num,
      ColumnVector column,
      TypeInfo columnType,
      LongColumnVector dictionaryIds) {
    System.arraycopy(dictionaryIds.isNull, rowId, column.isNull, rowId, num);
    if (column.noNulls) {
      column.noNulls = dictionaryIds.noNulls;
    }
    column.isRepeating = column.isRepeating && dictionaryIds.isRepeating;


    PrimitiveTypeInfo primitiveColumnType = (PrimitiveTypeInfo) columnType;

    switch (primitiveColumnType.getPrimitiveCategory()) {
    case INT:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((LongColumnVector) column).vector[i] =
                  dictionary.readInteger((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((LongColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case BYTE:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((LongColumnVector) column).vector[i] =
                  dictionary.readTinyInt((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((LongColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case SHORT:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((LongColumnVector) column).vector[i] =
                  dictionary.readSmallInt((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((LongColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case DATE:
      DateColumnVector dc = (DateColumnVector) column;
      dc.setUsingProlepticCalendar(true);
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          dc.vector[i] =
                  skipProlepticConversion ?
                          dictionary.readLong((int) dictionaryIds.vector[i]) :
                          CalendarUtils.convertDateToProleptic((int) dictionary.readLong((int) dictionaryIds.vector[i]));
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            dc.vector[i] = 0;
          }
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
    case LONG:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((LongColumnVector) column).vector[i] =
                  dictionary.readLong((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((LongColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case BOOLEAN:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((LongColumnVector) column).vector[i] =
                  dictionary.readBoolean((int) dictionaryIds.vector[i]) ? 1 : 0;
        }
      }
      break;
    case DOUBLE:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((DoubleColumnVector) column).vector[i] =
                  dictionary.readDouble((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((DoubleColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case BINARY:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((BytesColumnVector) column)
                  .setVal(i, dictionary.readBytes((int) dictionaryIds.vector[i]));
        }
      }
      break;
    case STRING:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((BytesColumnVector) column)
                  .setVal(i, dictionary.readString((int) dictionaryIds.vector[i]));
        }
      }
      break;
    case VARCHAR:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((BytesColumnVector) column)
                  .setVal(i, dictionary.readVarchar((int) dictionaryIds.vector[i]));
        }
      }
      break;
    case CHAR:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((BytesColumnVector) column)
                  .setVal(i, dictionary.readChar((int) dictionaryIds.vector[i]));
        }
      }
      break;
    case FLOAT:
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          ((DoubleColumnVector) column).vector[i] =
                  dictionary.readFloat((int) dictionaryIds.vector[i]);
          if (!dictionary.isValid()) {
            setNullValue(column, i);
            ((DoubleColumnVector) column).vector[i] = 0;
          }
        }
      }
      break;
    case DECIMAL:
      if (column instanceof Decimal64ColumnVector dec64) {
        fillDecimal64PrecisionScale(dec64);
        boolean fast = dictionary.isFastDecimal64();
        short valueScale = (short) getDecimalTypeInfo().getScale();
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNull[i]) {
            setDecimal64Value(dec64, i, fast, dictionary, (int) dictionaryIds.vector[i], valueScale);
          }
        }
        break;
      }
      DecimalColumnVector decimalColumnVector = ((DecimalColumnVector) column);
      byte[] decimalData;

      fillDecimalPrecisionScale(decimalColumnVector);

      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          decimalData = dictionary.readDecimal((int) dictionaryIds.vector[i]);
          if (dictionary.isValid()) {
            decimalColumnVector.vector[i].set(decimalData, decimalColumnVector.scale);
          } else {
            setNullValue(column, i);
          }
        }
      }
      break;
    case TIMESTAMP:
    case TIMESTAMPLOCALTZ:
      TimestampColumnVector tsc = (TimestampColumnVector) column;
      tsc.setUsingProlepticCalendar(true);
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          Timestamp t = dictionary.readTimestamp((int) dictionaryIds.vector[i]);
          if (dictionary.isValid()) {
            tsc.set(i, t.toSqlTimestamp());
          } else {
            setNullValue(column, i);
          }
        }
      }
      break;
    case INTERVAL_DAY_TIME:
    default:
      throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  /**
   * Fill a {@link DecimalColumnVector} at the Parquet file (logical-type) precision/scale: the scale
   * {@link #readDecimal} reads the unscaled bytes at, carried per row by the HiveDecimal.
   */
  private void fillDecimalPrecisionScale(DecimalColumnVector c) {
    DecimalTypeInfo dti = getDecimalTypeInfo();
    c.precision = (short) dti.getPrecision();
    c.scale = (short) dti.getScale();
  }

  /**
   * Fill a long-backed {@link Decimal64ColumnVector} at the Hive (table) scale -- the scale every
   * consumer reads {@code c.vector} at, and the only scale at which the unscaled value fits the long.
   * NOT the Parquet file scale from {@link #getDecimalTypeInfo()}: under schema evolution that
   * scale can be larger (e.g. a DECIMAL(38,37) file read as DECIMAL(16,8)) and would overflow.
   */
  private void fillDecimal64PrecisionScale(Decimal64ColumnVector c) {
    DecimalTypeInfo dti = hiveType instanceof DecimalTypeInfo hiveDti ? hiveDti : getDecimalTypeInfo();
    c.precision = (short) dti.getPrecision();
    c.scale = (short) dti.getScale();
  }

  /**
   * Decimal precision/scale for this column: from the Parquet decimal logical type when present,
   * otherwise from the Hive type (Parquet stores it as a non-decimal physical type but HMS reports
   * decimal).
   */
  private DecimalTypeInfo getDecimalTypeInfo() {
    if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation d) {
      return TypeInfoFactory.getDecimalTypeInfo(d.getPrecision(), d.getScale());
    } else if (TypeInfoUtils.getBaseName(hiveType.getTypeName())
        .equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) {
      return (DecimalTypeInfo) hiveType;
    }
    throw new UnsupportedOperationException(
        "The underlying Parquet type cannot be converted to Hive Decimal type: " + type);
  }

  /**
   * Decimal64 fast path: read the unscaled value straight into the long-backed vector instead of
   * materializing a HiveDecimal per row. Only for columns the vectorizer tagged DECIMAL_64
   * (precision <= 18); higher precision uses {@link #readDecimal}.
   */
  private void readDecimal64(int total, Decimal64ColumnVector c, int rowId) {
    fillDecimal64PrecisionScale(c);
    boolean fast = dataColumn.isFastDecimal64();
    short valueScale = (short) getDecimalTypeInfo().getScale();
    int left = total;
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        setDecimal64Value(c, rowId, fast, dataColumn, -1, valueScale);
        if (!c.isNull[rowId]) {
          c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
        }
      } else {
        setNullValue(c, rowId);
      }
      rowId++;
      left--;
    }
  }

  /**
   * Store one decimal64 value into {@code c[rowId]} from {@code reader}, NULLing the entry when the
   * value is out of range. {@code fast} selects the identity fast path (raw unscaled long) over the
   * HiveDecimal/byte[] slow path. {@code id >= 0} reads that dictionary entry; a negative {@code id}
   * reads the current page value. Shared by the page ({@link #readDecimal64}) and dictionary
   * ({@link #decodeDictionaryIds}) decode loops.
   */
  private void setDecimal64Value(Decimal64ColumnVector c, int rowId, boolean fast,
      ParquetDataColumnReader reader, int id, short valueScale) {
    c.isNull[rowId] = false;
    boolean stored;
    if (fast) {
      // Identity fast path: store the raw unscaled long directly (no HiveDecimal/byte[] per row).
      long v = id >= 0 ? reader.readDecimal64(id) : reader.readDecimal64();
      stored = reader.isValid();
      if (stored) {
        c.vector[rowId] = v;
      }
    } else {
      // set() enforces the column precision/scale and marks the entry NULL if the value does not
      // fit (e.g. schema-evolved data whose larger file scale can't be held at the column scale).
      byte[] bytes = id >= 0 ? reader.readDecimal(id) : reader.readDecimal();
      stored = reader.isValid();
      if (stored) {
        c.set(rowId, bytes, valueScale);
        stored = !c.isNull[rowId];
      }
    }
    if (!stored) {
      c.vector[rowId] = 0;
      setNullValue(c, rowId);
    }
  }
}
