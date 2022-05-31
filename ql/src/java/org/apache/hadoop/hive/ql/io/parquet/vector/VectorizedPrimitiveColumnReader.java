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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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
      readDecimal(num, (DecimalColumnVector) column, rowId);
      break;
    case TIMESTAMP:
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

  private void readDictionaryIDs(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readIntegers(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readSmallInts(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readTinyInts(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readDoubles(
      int total,
      DoubleColumnVector c,
      int rowId) throws IOException {
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

  private void readBooleans(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readLongs(
      int total,
      LongColumnVector c,
      int rowId) throws IOException {
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

  private void readFloats(
      int total,
      DoubleColumnVector c,
      int rowId) throws IOException {
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

  private void readDecimal(
      int total,
      DecimalColumnVector c,
      int rowId) throws IOException {

    DecimalLogicalTypeAnnotation decimalLogicalType = null;
    if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
      decimalLogicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
    }
    byte[] decimalData = null;
    fillDecimalPrecisionScale(decimalLogicalType, c);

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

  private void readString(
      int total,
      BytesColumnVector c,
      int rowId) throws IOException {
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

  private void readChar(
      int total,
      BytesColumnVector c,
      int rowId) throws IOException {
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

  private void readVarchar(
      int total,
      BytesColumnVector c,
      int rowId) throws IOException {
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

  private void readBinaries(
      int total,
      BytesColumnVector c,
      int rowId) throws IOException {
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

  private void readDate(
      int total,
      DateColumnVector c,
      int rowId) throws IOException {
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
          c.set(rowId, dataColumn.readTimestamp().toSqlTimestamp());
          break;
        case INT64:
          c.set(rowId, dataColumn.readTimestamp().toSqlTimestamp());
          break;
        default:
          throw new IOException(
              "Unsupported parquet logical type: " + type.getLogicalTypeAnnotation().toString() + " for timestamp");
        }
        c.isNull[rowId] = false;
        c.isRepeating =
            c.isRepeating && ((c.time[0] == c.time[rowId]) && (c.nanos[0] == c.nanos[rowId]));
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
      DecimalLogicalTypeAnnotation decimalLogicalType = null;
      if (type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
        decimalLogicalType = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
      }
      DecimalColumnVector decimalColumnVector = ((DecimalColumnVector) column);
      byte[] decimalData = null;

      fillDecimalPrecisionScale(decimalLogicalType, decimalColumnVector);

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
      TimestampColumnVector tsc = (TimestampColumnVector) column;
      tsc.setUsingProlepticCalendar(true);
      for (int i = rowId; i < rowId + num; ++i) {
        if (!column.isNull[i]) {
          tsc.set(i, dictionary.readTimestamp((int) dictionaryIds.vector[i]).toSqlTimestamp());
        }
      }
      break;
    case INTERVAL_DAY_TIME:
    default:
      throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  /**
   * The decimal precision and scale is filled into decimalColumnVector.  If the data in
   * Parquet is in decimal, the precision and scale will come in from decimalLogicalType.  If parquet
   * is not in decimal, then this call is made because HMS shows the type as decimal.  So, the
   * precision and scale are picked from hiveType.
   *
   * @param decimalLogicalType
   * @param decimalColumnVector
   */
  private void fillDecimalPrecisionScale(DecimalLogicalTypeAnnotation decimalLogicalType,
      DecimalColumnVector decimalColumnVector) {
    if (decimalLogicalType != null) {
      decimalColumnVector.precision = (short) decimalLogicalType.getPrecision();
      decimalColumnVector.scale = (short) decimalLogicalType.getScale();
    } else if (TypeInfoUtils.getBaseName(hiveType.getTypeName())
        .equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) {
      decimalColumnVector.precision = (short) ((DecimalTypeInfo) hiveType).getPrecision();
      decimalColumnVector.scale = (short) ((DecimalTypeInfo) hiveType).getScale();
    } else {
      throw new UnsupportedOperationException(
          "The underlying Parquet type cannot be converted to Hive Decimal type: " + type);
    }
  }
}
