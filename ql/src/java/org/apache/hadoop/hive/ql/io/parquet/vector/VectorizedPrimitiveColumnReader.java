/**
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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.Type;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;

/**
 * It's column level Parquet reader which is used to read a batch of records for a column,
 * part of the code is referred from Apache Spark and Apache Parquet.
 */
public class VectorizedPrimitiveColumnReader extends BaseVectorizedColumnReader {

  public VectorizedPrimitiveColumnReader(
    ColumnDescriptor descriptor,
    PageReader pageReader,
    boolean skipTimestampConversion,
    Type type) throws IOException {
    super(descriptor, pageReader, skipTimestampConversion, type);
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
        decodeDictionaryIds(rowId, num, column, dictionaryIds);
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
    case BYTE:
    case SHORT:
      readIntegers(num, (LongColumnVector) column, rowId);
      break;
    case DATE:
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
    case STRING:
    case CHAR:
    case VARCHAR:
      readBinaries(num, (BytesColumnVector) column, rowId);
      break;
    case FLOAT:
      readFloats(num, (DoubleColumnVector) column, rowId);
      break;
    case DECIMAL:
      readDecimal(num, (DecimalColumnVector) column, rowId);
      break;
    case INTERVAL_DAY_TIME:
    case TIMESTAMP:
    default:
      throw new IOException("Unsupported type: " + type);
    }
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
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  private void readDecimal(
    int total,
    DecimalColumnVector c,
    int rowId) throws IOException {
    int left = total;
    c.precision = (short) type.asPrimitiveType().getDecimalMetadata().getPrecision();
    c.scale = (short) type.asPrimitiveType().getDecimalMetadata().getScale();
    while (left > 0) {
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= maxDefLevel) {
        c.vector[rowId].set(dataColumn.readBytes().getBytesUnsafe(), c.scale);
        c.isNull[rowId] = false;
        c.isRepeating = c.isRepeating && (c.vector[0] == c.vector[rowId]);
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
        c.setVal(rowId, dataColumn.readBytes().getBytesUnsafe());
        c.isNull[rowId] = false;
        // TODO figure out a better way to set repeat for Binary type
        c.isRepeating = false;
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
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
    LongColumnVector dictionaryIds) {
    System.arraycopy(dictionaryIds.isNull, rowId, column.isNull, rowId, num);
    if (column.noNulls) {
      column.noNulls = dictionaryIds.noNulls;
    }
    column.isRepeating = column.isRepeating && dictionaryIds.isRepeating;

    switch (descriptor.getType()) {
    case INT32:
      for (int i = rowId; i < rowId + num; ++i) {
        ((LongColumnVector) column).vector[i] =
          dictionary.decodeToInt((int) dictionaryIds.vector[i]);
      }
      break;
    case INT64:
      for (int i = rowId; i < rowId + num; ++i) {
        ((LongColumnVector) column).vector[i] =
          dictionary.decodeToLong((int) dictionaryIds.vector[i]);
      }
      break;
    case FLOAT:
      for (int i = rowId; i < rowId + num; ++i) {
        ((DoubleColumnVector) column).vector[i] =
          dictionary.decodeToFloat((int) dictionaryIds.vector[i]);
      }
      break;
    case DOUBLE:
      for (int i = rowId; i < rowId + num; ++i) {
        ((DoubleColumnVector) column).vector[i] =
          dictionary.decodeToDouble((int) dictionaryIds.vector[i]);
      }
      break;
    case INT96:
      for (int i = rowId; i < rowId + num; ++i) {
        ByteBuffer buf = dictionary.decodeToBinary((int) dictionaryIds.vector[i]).toByteBuffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = buf.getLong();
        int julianDay = buf.getInt();
        NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
        Timestamp ts = NanoTimeUtils.getTimestamp(nt, skipTimestampConversion);
        ((TimestampColumnVector) column).set(i, ts);
      }
      break;
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      if (column instanceof BytesColumnVector) {
        for (int i = rowId; i < rowId + num; ++i) {
          ((BytesColumnVector) column)
            .setVal(i, dictionary.decodeToBinary((int) dictionaryIds.vector[i]).getBytesUnsafe());
        }
      } else {
        DecimalColumnVector decimalColumnVector = ((DecimalColumnVector) column);
        decimalColumnVector.precision =
          (short) type.asPrimitiveType().getDecimalMetadata().getPrecision();
        decimalColumnVector.scale = (short) type.asPrimitiveType().getDecimalMetadata().getScale();
        for (int i = rowId; i < rowId + num; ++i) {
          decimalColumnVector.vector[i]
            .set(dictionary.decodeToBinary((int) dictionaryIds.vector[i]).getBytesUnsafe(),
              decimalColumnVector.scale);
        }
      }
      break;
    default:
      throw new UnsupportedOperationException("Unsupported type: " + descriptor.getType());
    }
  }
}
