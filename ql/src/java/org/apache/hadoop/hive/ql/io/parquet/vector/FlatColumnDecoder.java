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
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.parquet.convert.ETypeConverter;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetDataColumnReaderFactory.TypesFromDecimalPageReader;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Batched decoder for a flat primitive column (no repetition, at most one definition level) whose
 * pages are PLAIN or dictionary encoded and whose Parquet type maps onto the Hive type without
 * conversion. Definition levels are decoded per batch straight into isNull[], PLAIN values are read
 * from the page buffer in bulk, and dictionary ids are unpacked in bulk and gathered from a
 * dictionary decoded once per column chunk. A dictionary-encoded BINARY column costs one setRef per
 * row against that decoded dictionary rather than a copy, the way ORC's encoded string reader does.
 *
 * Values, NULL entries, the decimal64 range check and noNulls match the per-value path of
 * {@link VectorizedPrimitiveColumnReader}. A long or double batch is repeating when all its values are
 * equal and none is NULL, on dictionary pages too, where the per-value path never flags one; a string or
 * decimal batch only when it has one row. Values under NULL entries are unspecified.
 */
final class FlatColumnDecoder {

  enum Kind {
    INT32, INT64, FLOAT, DOUBLE, DATE, BOOLEAN, BINARY, DECIMAL_INT32, DECIMAL_INT64, DECIMAL_FLBA;

    boolean decimal() {
      return this == DECIMAL_INT32 || this == DECIMAL_INT64 || this == DECIMAL_FLBA;
    }
  }

  private final Kind kind;
  private final Dictionary dictionary;
  private final int typeLength;
  private final short fileScale;
  private final int hivePrecision;
  private final int hiveScale;
  private final long decimal64AbsMax;
  private final boolean fastDecimal64;
  private final int truncateLength;
  private final boolean hybridDates;

  private final RleBitPackedIntDecoder levels;
  private final RleBitPackedIntDecoder booleans;
  private RleBitPackedIntDecoder ids;
  private int[] idBuf = new int[0];
  private final byte[] decimalBytes;
  private final HiveDecimalWritable decimalScratch;

  private ByteBuffer data;
  private int dataPos;
  private boolean pageDictionary;
  private boolean rleBooleans;
  private int plainBooleanBit;

  private long[] dictLongs;
  private double[] dictDoubles;
  private byte[][] dictDecimalBytes;
  private byte[] dictBinary;
  private int[] dictBinaryOffsets;
  private boolean dictHasInvalid;

  private int batchNulls;

  /**
   * The decoder for a column, or null when the column needs the per-value path.
   */
  static FlatColumnDecoder forColumn(ColumnDescriptor descriptor, PrimitiveType type, TypeInfo hiveType,
      Dictionary dictionary, boolean skipProlepticConversion) {
    if (descriptor.getMaxRepetitionLevel() != 0 || descriptor.getMaxDefinitionLevel() > 1
        || !(hiveType instanceof PrimitiveTypeInfo hive)) {
      return null;
    }
    Kind kind = kindOf(type, hive.getPrimitiveCategory());
    return kind == null ? null
        : new FlatColumnDecoder(kind, descriptor, type, hive, dictionary, skipProlepticConversion);
  }

  /**
   * TIMESTAMP, TINYINT, SMALLINT and the unsigned integers are not covered yet. Each needs its
   * per-value reader's extra step carried into the batch: a timezone and calendar conversion for
   * INT96, and for the narrow and unsigned integers a range check that NULLs what the Hive type
   * cannot hold, since Parquet stores them in a wider physical type. None of that prevents
   * batching -- Trino decodes all of them a batch at a time, doing the same work inside the loop
   * -- so these are gaps to close rather than types that have to stay per-value.
   */
  private static Kind kindOf(PrimitiveType type, PrimitiveCategory hive) {
    boolean decimal = type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation;
    return switch (type.getPrimitiveTypeName()) {
      case INT32 -> {
        if (decimal) {
          yield hive == PrimitiveCategory.DECIMAL ? Kind.DECIMAL_INT32 : null;
        }
        if (ETypeConverter.isUnsignedInteger(type)) {
          yield null;
        }
        yield switch (hive) {
          case INT, LONG -> Kind.INT32;
          case DATE -> Kind.DATE;
          default -> null;
        };
      }
      case INT64 -> decimal ? (hive == PrimitiveCategory.DECIMAL ? Kind.DECIMAL_INT64 : null)
          : !ETypeConverter.isUnsignedInteger(type) && hive == PrimitiveCategory.LONG ? Kind.INT64 : null;
      case FLOAT -> hive == PrimitiveCategory.FLOAT ? Kind.FLOAT : null;
      case DOUBLE -> hive == PrimitiveCategory.DOUBLE ? Kind.DOUBLE : null;
      case BOOLEAN -> hive == PrimitiveCategory.BOOLEAN ? Kind.BOOLEAN : null;
      case BINARY -> decimal ? null : switch (hive) {
        case STRING, CHAR, VARCHAR, BINARY -> Kind.BINARY;
        default -> null;
      };
      case FIXED_LEN_BYTE_ARRAY -> decimal && hive == PrimitiveCategory.DECIMAL ? Kind.DECIMAL_FLBA : null;
      // INT96, the only physical type left, carries a timestamp.
      default -> null;
    };
  }

  private FlatColumnDecoder(Kind kind, ColumnDescriptor descriptor, PrimitiveType type, PrimitiveTypeInfo hiveType,
      Dictionary dictionary, boolean skipProlepticConversion) {
    this.kind = kind;
    this.dictionary = dictionary;
    this.levels = descriptor.getMaxDefinitionLevel() == 0 ? null : new RleBitPackedIntDecoder(1);
    this.booleans = kind == Kind.BOOLEAN ? new RleBitPackedIntDecoder(1) : null;
    this.hybridDates = kind == Kind.DATE && !skipProlepticConversion;
    // Only a UTF8-annotated column truncates, matching TypesFromStringPageReader; an unannotated
    // BINARY reads whole even into CHAR/VARCHAR. Neither pads a CHAR out to its length.
    this.truncateLength = type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation
        && hiveType instanceof BaseCharTypeInfo charType ? charType.getLength() : -1;
    if (kind.decimal()) {
      DecimalLogicalTypeAnnotation file = (DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
      DecimalTypeInfo hive = (DecimalTypeInfo) hiveType;
      typeLength = switch (kind) {
        case DECIMAL_INT32 -> 4;
        case DECIMAL_INT64 -> 8;
        default -> type.getTypeLength();
      };
      fileScale = (short) file.getScale();
      hivePrecision = hive.getPrecision();
      hiveScale = hive.getScale();
      decimal64AbsMax = HiveDecimalWritable.isPrecisionDecimal64(hivePrecision)
          ? HiveDecimalWritable.getDecimal64AbsMax(hivePrecision) : 0;
      fastDecimal64 = fileScale == hiveScale && HiveDecimalWritable.isPrecisionDecimal64(file.getPrecision());
      decimalBytes = new byte[typeLength];
      decimalScratch = new HiveDecimalWritable(0L);
    } else {
      typeLength = 0;
      fileScale = 0;
      hivePrecision = 0;
      hiveScale = 0;
      decimal64AbsMax = 0;
      fastDecimal64 = false;
      decimalBytes = null;
      decimalScratch = null;
    }
  }

  /**
   * A decimal64 target needs the identity fast path (same scale, precision <= 18); rescaled decimals
   * keep the per-value path.
   */
  boolean accepts(ColumnVector column) {
    return !(column instanceof Decimal64ColumnVector) || fastDecimal64;
  }

  /**
   * Set up the page for batched reads; false leaves the page untouched for the per-value path.
   */
  boolean initPage(DataPage page) {
    try {
      if (page instanceof DataPageV1 v1) {
        return initPageV1(v1);
      }
      return initPageV2((DataPageV2) page);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page, e);
    }
  }

  /**
   * Level streams: none for a max level of 0 (parquet-mr labels them BIT_PACKED), a length-prefixed
   * RLE stream for a definition level of 1.
   */
  private boolean initPageV1(DataPageV1 page) throws IOException {
    Encoding encoding = page.getValueEncoding();
    if (!accepts(encoding) || (levels != null && page.getDlEncoding() != Encoding.RLE)) {
      return false;
    }
    ByteBufferInputStream in = page.getBytes().toInputStream();
    if (levels != null) {
      int length = BytesUtils.readIntLittleEndian(in);
      ByteBuffer levelBytes = in.slice(length);
      levels.reset(levelBytes, levelBytes.position(), levelBytes.limit());
    }
    initValues(encoding, in.slice(in.available()));
    return true;
  }

  private boolean initPageV2(DataPageV2 page) throws IOException {
    Encoding encoding = page.getDataEncoding();
    if (!accepts(encoding)) {
      return false;
    }
    if (levels != null) {
      ByteBuffer levelBytes = page.getDefinitionLevels().toByteBuffer();
      levels.reset(levelBytes, levelBytes.position(), levelBytes.limit());
    }
    initValues(encoding, page.getData().toByteBuffer());
    return true;
  }

  /** RLE is the page v2 encoding of a BOOLEAN column; for every other type it encodes levels only. */
  private boolean accepts(Encoding encoding) {
    return encoding == Encoding.PLAIN || (encoding == Encoding.RLE && kind == Kind.BOOLEAN)
        || (encoding.usesDictionary() && dictionary != null);
  }

  private void initValues(Encoding encoding, ByteBuffer values) {
    data = values;
    dataPos = values.position();
    pageDictionary = encoding.usesDictionary();
    if (pageDictionary) {
      // An all-null page may carry no ids at all, not even the bit width byte.
      int bitWidth = dataPos < values.limit() ? values.get(dataPos) & 0xFF : 0;
      if (ids == null || ids.bitWidth() != bitWidth) {
        ids = new RleBitPackedIntDecoder(bitWidth);
      }
      ids.reset(values, Math.min(dataPos + 1, values.limit()), values.limit());
      return;
    }
    data.order(kind == Kind.DECIMAL_FLBA ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    if (kind == Kind.BOOLEAN) {
      rleBooleans = encoding == Encoding.RLE;
      plainBooleanBit = 0;
      if (rleBooleans) {
        int length = data.getInt(dataPos);
        booleans.reset(values, dataPos + 4, dataPos + 4 + length);
      }
    }
  }

  void beginBatch(int total) {
    batchNulls = 0;
    if (idBuf.length < total) {
      idBuf = new int[total];
    }
  }

  /**
   * Decode the next {@code num} values of the current page into {@code column[rowId ..)}, recording
   * their definition levels in {@code defLevels[rowId ..)}.
   */
  void readValues(int num, ColumnVector column, int rowId, int[] defLevels) {
    boolean[] isNull = column.isNull;
    int end = rowId + num;
    int nulls = 0;
    if (levels == null) {
      Arrays.fill(isNull, rowId, end, false);
    } else {
      levels.read(defLevels, rowId, num);
      for (int i = rowId; i < end; i++) {
        boolean n = defLevels[i] == 0;
        isNull[i] = n;
        nulls += n ? 1 : 0;
      }
    }
    int nonNull = num - nulls;
    int invalid;
    if (pageDictionary) {
      ids.read(idBuf, 0, nonNull);
      invalid = readDictionary(column, rowId, num, nonNull);
    } else {
      invalid = readPlain(column, rowId, num, nonNull);
    }
    batchNulls += nulls + invalid;
  }

  /** A batch with a NULL is never repeating; otherwise it is repeating when every value is equal. */
  void finishBatch(ColumnVector column, int total) {
    if (batchNulls > 0) {
      column.noNulls = false;
      column.isRepeating = false;
    } else if (column.isRepeating && total > 0) {
      column.isRepeating = allEqual(column, total);
    }
  }

  private static boolean allEqual(ColumnVector column, int total) {
    if (column instanceof LongColumnVector c) {
      long first = c.vector[0];
      for (int i = 1; i < total; i++) {
        if (c.vector[i] != first) {
          return false;
        }
      }
      return true;
    }
    if (column instanceof DoubleColumnVector c) {
      double first = c.vector[0];
      for (int i = 1; i < total; i++) {
        if (c.vector[i] != first) {
          return false;
        }
      }
      return true;
    }
    // The per-value path never reports a BytesColumnVector batch repeating.
    return !(column instanceof BytesColumnVector) && total == 1;
  }

  // PLAIN pages: read the non-null values densely, then spread them over the NULL entries.

  private int readPlain(ColumnVector column, int rowId, int num, int nonNull) {
    int p = dataPos;
    switch (kind) {
      case INT32 -> {
        long[] v = ((LongColumnVector) column).vector;
        for (int j = rowId; j < rowId + nonNull; j++, p += 4) {
          v[j] = data.getInt(p);
        }
        dataPos = p;
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case INT64 -> {
        long[] v = ((LongColumnVector) column).vector;
        for (int j = rowId; j < rowId + nonNull; j++, p += 8) {
          v[j] = data.getLong(p);
        }
        dataPos = p;
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case FLOAT -> {
        double[] v = ((DoubleColumnVector) column).vector;
        for (int j = rowId; j < rowId + nonNull; j++, p += 4) {
          v[j] = data.getFloat(p);
        }
        dataPos = p;
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case DOUBLE -> {
        double[] v = ((DoubleColumnVector) column).vector;
        for (int j = rowId; j < rowId + nonNull; j++, p += 8) {
          v[j] = data.getDouble(p);
        }
        dataPos = p;
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case DATE -> {
        long[] v = ((LongColumnVector) column).vector;
        int end = rowId + nonNull;
        for (int j = rowId; j < end; j++, p += 4) {
          v[j] = data.getInt(p);
        }
        dataPos = p;
        if (hybridDates) {
          toProleptic(v, rowId, end);
        }
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case BOOLEAN -> {
        long[] v = ((LongColumnVector) column).vector;
        readBooleans(v, rowId, nonNull);
        if (nonNull < num) {
          scatter(v, column.isNull, rowId, num, nonNull);
        }
      }
      case BINARY -> readPlainBinary((BytesColumnVector) column, rowId, num);
      case DECIMAL_INT32, DECIMAL_INT64, DECIMAL_FLBA -> {
        return column instanceof Decimal64ColumnVector c
            ? readPlainDecimal64(c, rowId, num, nonNull)
            : readPlainDecimal((DecimalColumnVector) column, rowId, num);
      }
    }
    return 0;
  }

  /** Hybrid Julian/Gregorian day numbers as the per-value path converts them. */
  private static void toProleptic(long[] v, int from, int to) {
    for (int i = from; i < to; i++) {
      v[i] = CalendarUtils.convertDateToProleptic((int) v[i]);
    }
  }

  /**
   * A PLAIN page holds the booleans as raw LSB-first bits with no run headers; page v2 wraps the
   * same values in a bit-width-1 hybrid stream, which lands in the dictionary id scratch.
   */
  private void readBooleans(long[] v, int rowId, int nonNull) {
    if (rleBooleans) {
      booleans.read(idBuf, 0, nonNull);
      for (int i = 0; i < nonNull; i++) {
        v[rowId + i] = idBuf[i];
      }
      return;
    }
    int bit = plainBooleanBit;
    for (int j = rowId; j < rowId + nonNull; j++, bit++) {
      v[j] = (data.get(dataPos + (bit >>> 3)) >>> (bit & 7)) & 1;
    }
    plainBooleanBit = bit;
  }

  /**
   * Length-prefixed values copied straight into the column's own buffer: the page bytes are cache
   * buffers the consumer releases after decode, so a PLAIN value can never be referenced in place.
   */
  private void readPlainBinary(BytesColumnVector c, int rowId, int num) {
    int p = dataPos;
    for (int i = rowId; i < rowId + num; i++) {
      if (c.isNull[i]) {
        continue;
      }
      int length = data.getInt(p);
      p += 4;
      c.ensureValPreallocated(length);
      byte[] target = c.getValPreallocatedBytes();
      int offset = c.getValPreallocatedStart();
      data.get(p, target, offset, length);
      p += length;
      c.setValPreallocated(i,
          truncateLength > 0 ? StringExpr.truncate(target, offset, length, truncateLength) : length);
    }
    dataPos = p;
  }

  private static void scatter(long[] v, boolean[] isNull, int rowId, int num, int nonNull) {
    for (int i = rowId + num - 1, j = rowId + nonNull - 1; i >= rowId; i--) {
      if (!isNull[i]) {
        v[i] = v[j--];
      }
    }
  }

  private static void scatter(double[] v, boolean[] isNull, int rowId, int num, int nonNull) {
    for (int i = rowId + num - 1, j = rowId + nonNull - 1; i >= rowId; i--) {
      if (!isNull[i]) {
        v[i] = v[j--];
      }
    }
  }

  /**
   * Big-endian two's-complement unscaled values straight into the long vector; entries outside the
   * Hive precision become NULL with value 0.
   */
  private int readPlainDecimal64(Decimal64ColumnVector c, int rowId, int num, int nonNull) {
    long[] v = c.vector;
    long absMax = decimal64AbsMax;
    boolean anyInvalid = false;
    int p = dataPos;
    int end = rowId + nonNull;
    switch (typeLength) {
      case 4 -> {
        for (int j = rowId; j < end; j++, p += 4) {
          long x = data.getInt(p);
          v[j] = x;
          anyInvalid |= (x < -absMax) | (x > absMax);
        }
      }
      case 8 -> {
        for (int j = rowId; j < end; j++, p += 8) {
          long x = data.getLong(p);
          v[j] = x;
          anyInvalid |= (x < -absMax) | (x > absMax);
        }
      }
      default -> {
        for (int j = rowId; j < end; j++, p += typeLength) {
          long x = data.get(p) < 0 ? -1L : 0L;
          for (int k = 0; k < typeLength; k++) {
            x = (x << 8) | (data.get(p + k) & 0xFF);
          }
          v[j] = x;
          anyInvalid |= (x < -absMax) | (x > absMax);
        }
      }
    }
    dataPos = p;
    if (nonNull < num) {
      scatter(v, c.isNull, rowId, num, nonNull);
    }
    return anyInvalid ? nullOutOfRange(c, rowId, num) : 0;
  }

  private int nullOutOfRange(Decimal64ColumnVector c, int rowId, int num) {
    int invalid = 0;
    for (int i = rowId; i < rowId + num; i++) {
      if (!c.isNull[i] && (c.vector[i] < -decimal64AbsMax || c.vector[i] > decimal64AbsMax)) {
        c.vector[i] = 0;
        c.isNull[i] = true;
        invalid++;
      }
    }
    return invalid;
  }

  private int readPlainDecimal(DecimalColumnVector c, int rowId, int num) {
    int invalid = 0;
    int p = dataPos;
    for (int i = rowId; i < rowId + num; i++) {
      if (c.isNull[i]) {
        continue;
      }
      byte[] validated = switch (kind) {
        case DECIMAL_INT32 -> validatedDecimalBytes(data.getInt(p));
        case DECIMAL_INT64 -> validatedDecimalBytes(data.getLong(p));
        default -> {
          data.get(p, decimalBytes);
          yield validatedDecimalBytes(decimalBytes);
        }
      };
      p += typeLength;
      if (validated == null) {
        c.isNull[i] = true;
        invalid++;
      } else {
        c.vector[i].set(validated, fileScale);
      }
    }
    dataPos = p;
    return invalid;
  }

  /** The file-scale value enforced at the Hive precision/scale, as bytes at the file scale again. */
  private byte[] validatedDecimalBytes(byte[] bytes) {
    decimalScratch.set(bytes, fileScale);
    return enforcedDecimalBytes();
  }

  private byte[] validatedDecimalBytes(long unscaled) {
    decimalScratch.setFromLongAndScale(unscaled, fileScale);
    return enforcedDecimalBytes();
  }

  private byte[] enforcedDecimalBytes() {
    decimalScratch.mutateEnforcePrecisionScale(hivePrecision, hiveScale);
    return decimalScratch.isSet() ? decimalScratch.getHiveDecimal().bigIntegerBytesScaled(fileScale) : null;
  }

  // Dictionary pages: unpack the ids of the non-null entries, then gather from the decoded dictionary.

  private int readDictionary(ColumnVector column, int rowId, int num, int nonNull) {
    switch (kind) {
      case INT32, INT64, DATE, BOOLEAN ->
          gather(dictLongs(), ((LongColumnVector) column).vector, column.isNull, rowId, num, nonNull);
      case FLOAT, DOUBLE -> gather(dictDoubles(), ((DoubleColumnVector) column).vector, column.isNull, rowId, num,
          nonNull);
      case BINARY -> gatherBinary((BytesColumnVector) column, rowId, num);
      case DECIMAL_INT32, DECIMAL_INT64, DECIMAL_FLBA -> {
        if (column instanceof Decimal64ColumnVector c) {
          gather(dictLongs(), c.vector, c.isNull, rowId, num, nonNull);
          return dictHasInvalid ? nullOutOfRange(c, rowId, num) : 0;
        }
        return gatherDecimal((DecimalColumnVector) column, rowId, num);
      }
    }
    return 0;
  }

  private void gather(long[] dict, long[] v, boolean[] isNull, int rowId, int num, int nonNull) {
    int[] id = idBuf;
    if (nonNull == num) {
      for (int i = 0; i < num; i++) {
        v[rowId + i] = dict[id[i]];
      }
    } else {
      for (int i = rowId, j = 0; i < rowId + num; i++) {
        if (!isNull[i]) {
          v[i] = dict[id[j++]];
        }
      }
    }
  }

  private void gather(double[] dict, double[] v, boolean[] isNull, int rowId, int num, int nonNull) {
    int[] id = idBuf;
    if (nonNull == num) {
      for (int i = 0; i < num; i++) {
        v[rowId + i] = dict[id[i]];
      }
    } else {
      for (int i = rowId, j = 0; i < rowId + num; i++) {
        if (!isNull[i]) {
          v[i] = dict[id[j++]];
        }
      }
    }
  }

  private void gatherBinary(BytesColumnVector c, int rowId, int num) {
    byte[] values = dictBinary();
    int[] offsets = dictBinaryOffsets;
    for (int i = rowId, j = 0; i < rowId + num; i++) {
      if (!c.isNull[i]) {
        int id = idBuf[j++];
        c.setRef(i, values, offsets[id], offsets[id + 1] - offsets[id]);
      }
    }
  }

  private int gatherDecimal(DecimalColumnVector c, int rowId, int num) {
    byte[][] dict = dictDecimalBytes();
    int invalid = 0;
    for (int i = rowId, j = 0; i < rowId + num; i++) {
      if (c.isNull[i]) {
        continue;
      }
      byte[] bytes = dict[idBuf[j++]];
      if (bytes == null) {
        c.isNull[i] = true;
        invalid++;
      } else {
        c.vector[i].set(bytes, fileScale);
      }
    }
    return invalid;
  }

  private long[] dictLongs() {
    if (dictLongs == null) {
      long[] d = new long[dictionary.getMaxId() + 1];
      for (int i = 0; i < d.length; i++) {
        d[i] = switch (kind) {
          case INT32 -> dictionary.decodeToInt(i);
          case INT64 -> dictionary.decodeToLong(i);
          case DATE -> hybridDates ? CalendarUtils.convertDateToProleptic(dictionary.decodeToInt(i))
              : dictionary.decodeToInt(i);
          case BOOLEAN -> dictionary.decodeToBoolean(i) ? 1 : 0;
          case DECIMAL_INT32 -> checkedDecimal64(dictionary.decodeToInt(i));
          case DECIMAL_INT64 -> checkedDecimal64(dictionary.decodeToLong(i));
          default -> checkedDecimal64(
              TypesFromDecimalPageReader.binaryToUnscaledLong(dictionary.decodeToBinary(i)));
        };
      }
      dictLongs = d;
    }
    return dictLongs;
  }

  private long checkedDecimal64(long unscaled) {
    dictHasInvalid |= unscaled < -decimal64AbsMax || unscaled > decimal64AbsMax;
    return unscaled;
  }

  private double[] dictDoubles() {
    if (dictDoubles == null) {
      double[] d = new double[dictionary.getMaxId() + 1];
      for (int i = 0; i < d.length; i++) {
        d[i] = kind == Kind.FLOAT ? dictionary.decodeToFloat(i) : dictionary.decodeToDouble(i);
      }
      dictDoubles = d;
    }
    return dictDoubles;
  }

  /**
   * The dictionary flattened once per column chunk into one array this decoder owns, with the
   * CHAR/VARCHAR truncation applied here instead of per row; rows reference it rather than copying,
   * which the per-value path cannot do because it sees one Binary at a time. Referencing is safe
   * only because the array is ours: the page buffers it was decoded from are cache buffers the
   * consumer releases once the chunk is decoded, while this one stays reachable from every
   * BytesColumnVector that points into it.
   */
  private byte[] dictBinary() {
    if (dictBinary == null) {
      int count = dictionary.getMaxId() + 1;
      byte[][] entries = new byte[count][];
      int[] offsets = new int[count + 1];
      for (int i = 0; i < count; i++) {
        byte[] bytes = dictionary.decodeToBinary(i).getBytesUnsafe();
        entries[i] = bytes;
        offsets[i + 1] = offsets[i]
            + (truncateLength > 0 ? StringExpr.truncate(bytes, 0, bytes.length, truncateLength) : bytes.length);
      }
      byte[] all = new byte[offsets[count]];
      for (int i = 0; i < count; i++) {
        System.arraycopy(entries[i], 0, all, offsets[i], offsets[i + 1] - offsets[i]);
      }
      dictBinaryOffsets = offsets;
      dictBinary = all;
    }
    return dictBinary;
  }

  private byte[][] dictDecimalBytes() {
    if (dictDecimalBytes == null) {
      byte[][] d = new byte[dictionary.getMaxId() + 1][];
      for (int i = 0; i < d.length; i++) {
        d[i] = switch (kind) {
          case DECIMAL_INT32 -> validatedDecimalBytes(dictionary.decodeToInt(i));
          case DECIMAL_INT64 -> validatedDecimalBytes(dictionary.decodeToLong(i));
          default -> validatedDecimalBytes(dictionary.decodeToBinary(i).getBytesUnsafe());
        };
      }
      dictDecimalBytes = d;
    }
    return dictDecimalBytes;
  }
}
