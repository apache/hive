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
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.hadoop.hive.ql.io.parquet.vector.PerValueUpdater.setNullValue;

/**
 * The dictionary of a column chunk as a vector with a row per entry, converted on first use by the per-value path's
 * own dictionary decode, then gathered into each batch by id. One subclass per kind of Hive column vector, which also
 * holds what depends only on that kind: the vector's set-up per batch, and whether a batch repeats.
 */
abstract class DictionaryValues {

  private final ParquetDataColumnReader dictionary;
  private final PrimitiveType type;
  private final TypeInfo hiveType;
  private final boolean skipProlepticConversion;
  final DecimalPrecisionScale decimals;
  private ColumnVector entries;
  private RuntimeException[] errors;

  DictionaryValues(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType,
      boolean skipProlepticConversion) {
    this.dictionary = dictionary;
    this.type = type;
    this.hiveType = hiveType;
    this.skipProlepticConversion = skipProlepticConversion;
    this.decimals = new DecimalPrecisionScale(type, hiveType);
  }

  /** Sets up the vector for a batch. */
  public void beginBatch(ColumnVector column) {
  }

  /** Fills the non-NULL rows of {@code [offset, offset + total)} from dictionary ids {@code ids[0 .. nonNull)}. */
  public abstract void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids);

  /**
   * Whether a batch of {@code total} rows, none of them NULL, holds a single value. {@code dictionaryOnly} says every
   * row came from a dictionary page, and then {@code sameIds} whether every row read the same id; otherwise the
   * values are compared, as the per-value path does.
   */
  public boolean isRepeating(ColumnVector column, int total, boolean dictionaryOnly, boolean sameIds) {
    return dictionaryOnly ? sameIds : allEqual(column, total);
  }

  boolean allEqual(ColumnVector column, int total) {
    long[] v = ((LongColumnVector) column).vector;
    for (int i = 1; i < total; i++) {
      if (v[i] != v[0]) {
        return false;
      }
    }
    return true;
  }

  abstract ColumnVector newEntries(int size);

  /**
   * The dictionary converted in one pass. If a conversion fails, the entries are converted one at a time instead, and
   * each failing entry keeps its exception, thrown when a row reads that entry, as the per-value path would. An entry
   * the conversion rejects is a NULL row.
   */
  final ColumnVector entries(int nonNull, int[] ids) {
    if (entries == null) {
      int size = dictionary.getDictionary().getMaxId() + 1;
      ColumnVector v = newEntries(size);
      LongColumnVector identity = new LongColumnVector(size);
      for (int id = 0; id < size; id++) {
        identity.vector[id] = id;
      }
      try {
        decodeDictionaryIds(0, size, v, hiveType, identity);
      } catch (RuntimeException e) {
        errors = new RuntimeException[size];
        for (int id = 0; id < size; id++) {
          try {
            decodeDictionaryIds(id, 1, v, hiveType, identity);
          } catch (RuntimeException entryError) {
            errors[id] = entryError;
          }
        }
      }
      entries = v;
    }
    for (int j = 0; errors != null && j < nonNull; j++) {
      if (errors[ids[j]] != null) {
        throw errors[ids[j]];
      }
    }
    return entries;
  }

  /** NULLs the rows of {@code [offset, offset + total)} whose entry is NULL. */
  static void nullInvalid(ColumnVector entries, int total, int offset, ColumnVector column, int[] ids) {
    if (entries.noNulls) {
      return;
    }
    for (int i = offset, j = 0; i < offset + total; i++) {
      if (!column.isNull[i] && entries.isNull[ids[j++]]) {
        column.isNull[i] = true;
        column.noNulls = false;
      }
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
        decimals.fillDecimal64PrecisionScale(dec64);
        boolean fast = dictionary.isFastDecimal64();
        short valueScale = (short) decimals.getDecimalTypeInfo().getScale();
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNull[i]) {
            DecimalPrecisionScale.setDecimal64Value(dec64, i, fast, dictionary, (int) dictionaryIds.vector[i],
                valueScale);
          }
        }
        break;
      }
      DecimalColumnVector decimalColumnVector = ((DecimalColumnVector) column);
      byte[] decimalData;

      decimals.fillDecimalPrecisionScale(decimalColumnVector);

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

  /** Long vectors: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, INTERVAL_YEAR_MONTH, DATE and decimal64. */
  static class Longs extends DictionaryValues {
    private final boolean date;

    Longs(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType, boolean skipProlepticConversion) {
      super(dictionary, type, hiveType, skipProlepticConversion);
      this.date = ((PrimitiveTypeInfo) hiveType).getPrimitiveCategory() == PrimitiveCategory.DATE;
    }

    @Override
    public void beginBatch(ColumnVector column) {
      if (date) {
        ((DateColumnVector) column).setUsingProlepticCalendar(true);
      }
    }

    @Override
    ColumnVector newEntries(int size) {
      return date ? new DateColumnVector(size) : new LongColumnVector(size);
    }

    @Override
    public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
      LongColumnVector e = (LongColumnVector) entries(nonNull, ids);
      long[] v = ((LongColumnVector) column).vector;
      if (nonNull == total) {
        for (int i = 0; i < total; i++) {
          v[offset + i] = e.vector[ids[i]];
        }
      } else {
        for (int i = offset, j = 0; i < offset + total; i++) {
          if (!column.isNull[i]) {
            v[i] = e.vector[ids[j++]];
          }
        }
      }
      nullInvalid(e, total, offset, column, ids);
    }
  }

  /** Decimal64 vectors, at the Hive precision and scale. */
  static class Decimal64s extends Longs {

    Decimal64s(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType, false);
    }

    @Override
    public void beginBatch(ColumnVector column) {
      decimals.fillDecimal64PrecisionScale((Decimal64ColumnVector) column);
    }

    @Override
    ColumnVector newEntries(int size) {
      return new Decimal64ColumnVector(size, 0, 0);
    }
  }

  /** Double vectors: FLOAT and DOUBLE. */
  static class Doubles extends DictionaryValues {

    Doubles(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType, false);
    }

    @Override
    ColumnVector newEntries(int size) {
      return new DoubleColumnVector(size);
    }

    @Override
    public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
      DoubleColumnVector e = (DoubleColumnVector) entries(nonNull, ids);
      double[] v = ((DoubleColumnVector) column).vector;
      if (nonNull == total) {
        for (int i = 0; i < total; i++) {
          v[offset + i] = e.vector[ids[i]];
        }
      } else {
        for (int i = offset, j = 0; i < offset + total; i++) {
          if (!column.isNull[i]) {
            v[i] = e.vector[ids[j++]];
          }
        }
      }
      nullInvalid(e, total, offset, column, ids);
    }

    /** Compares the bits: 0.0 and -0.0 are equal doubles but different values. */
    @Override
    boolean allEqual(ColumnVector column, int total) {
      double[] v = ((DoubleColumnVector) column).vector;
      long first = Double.doubleToLongBits(v[0]);
      for (int i = 1; i < total; i++) {
        if (Double.doubleToLongBits(v[i]) != first) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Bytes vectors: STRING, CHAR, VARCHAR and BINARY. The entries are copied into one array this reader owns, and a
   * row references its entry rather than copying it: the page buffers the entries were decoded from are cache buffers
   * the consumer releases once the chunk is decoded.
   */
  static class Bytes extends DictionaryValues {
    private byte[] values;
    private int[] offsets;

    Bytes(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType, false);
    }

    @Override
    ColumnVector newEntries(int size) {
      BytesColumnVector entries = new BytesColumnVector(size);
      entries.init();
      return entries;
    }

    @Override
    public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
      BytesColumnVector e = (BytesColumnVector) entries(nonNull, ids);
      if (values == null) {
        copyEntries(e);
      }
      BytesColumnVector c = (BytesColumnVector) column;
      byte[] bytes = values;
      int[] starts = offsets;
      for (int i = offset, j = 0; i < offset + total; i++) {
        if (!c.isNull[i]) {
          int id = ids[j++];
          c.setRef(i, bytes, starts[id], starts[id + 1] - starts[id]);
        }
      }
    }

    private void copyEntries(BytesColumnVector e) {
      offsets = new int[e.vector.length + 1];
      for (int id = 0; id < e.vector.length; id++) {
        offsets[id + 1] = offsets[id] + e.length[id];
      }
      values = new byte[offsets[e.vector.length]];
      for (int id = 0; id < e.vector.length; id++) {
        // an entry whose conversion failed has no value: its rows throw its error (entries)
        if (e.vector[id] != null) {
          System.arraycopy(e.vector[id], e.start[id], values, offsets[id], e.length[id]);
        }
      }
    }

    /** As on PLAIN pages, a string batch is never flagged repeating. */
    @Override
    public boolean isRepeating(ColumnVector column, int total, boolean dictionaryOnly, boolean sameIds) {
      return false;
    }
  }

  /** HiveDecimal vectors, at the file precision and scale. */
  static class Decimals extends DictionaryValues {

    Decimals(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType, false);
    }

    @Override
    public void beginBatch(ColumnVector column) {
      decimals.fillDecimalPrecisionScale((DecimalColumnVector) column);
    }

    @Override
    ColumnVector newEntries(int size) {
      return new DecimalColumnVector(size, 0, 0);
    }

    @Override
    public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
      DecimalColumnVector e = (DecimalColumnVector) entries(nonNull, ids);
      DecimalColumnVector c = (DecimalColumnVector) column;
      for (int i = offset, j = 0; i < offset + total; i++) {
        if (!c.isNull[i]) {
          int id = ids[j++];
          if (e.isNull[id]) {
            setNullValue(c, i);
          } else {
            c.vector[i].set(e.vector[id]);
          }
        }
      }
    }

    /** As on PLAIN pages, a HiveDecimal batch repeats only when it has one row. */
    @Override
    public boolean isRepeating(ColumnVector column, int total, boolean dictionaryOnly, boolean sameIds) {
      return total == 1;
    }
  }

  /** Timestamp vectors. */
  static class Timestamps extends DictionaryValues {

    Timestamps(ParquetDataColumnReader dictionary, PrimitiveType type, TypeInfo hiveType) {
      super(dictionary, type, hiveType, false);
    }

    @Override
    public void beginBatch(ColumnVector column) {
      ((TimestampColumnVector) column).setUsingProlepticCalendar(true);
    }

    @Override
    ColumnVector newEntries(int size) {
      return new TimestampColumnVector(size);
    }

    @Override
    public void decodeDictionaryIds(int total, int nonNull, int offset, ColumnVector column, int[] ids) {
      TimestampColumnVector e = (TimestampColumnVector) entries(nonNull, ids);
      TimestampColumnVector c = (TimestampColumnVector) column;
      for (int i = offset, j = 0; i < offset + total; i++) {
        if (!c.isNull[i]) {
          int id = ids[j++];
          c.time[i] = e.time[id];
          c.nanos[i] = e.nanos[id];
        }
      }
    }

    @Override
    boolean allEqual(ColumnVector column, int total) {
      TimestampColumnVector c = (TimestampColumnVector) column;
      for (int i = 1; i < total; i++) {
        if (c.time[i] != c.time[0] || c.nanos[i] != c.nanos[0]) {
          return false;
        }
      }
      return true;
    }
  }
}
