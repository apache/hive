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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.io.parquet.convert.ETypeConverter;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.time.ZoneId;

/**
 * Picks the updater of a column chunk from its Parquet type, its Hive type and the column vector it fills.
 */
final class ParquetVectorUpdaterFactory {

  private final ColumnDescriptor descriptor;
  private final PrimitiveType type;
  private final TypeInfo hiveType;
  private final ParquetDataColumnReader dictionary;
  private final boolean skipTimestampConversion;
  private final ZoneId writerTimezone;
  private final boolean skipProlepticConversion;
  private final boolean legacyConversionEnabled;

  ParquetVectorUpdaterFactory(ColumnDescriptor descriptor, PrimitiveType type, TypeInfo hiveType,
      DictionaryPage dictionaryPage, boolean skipTimestampConversion, ZoneId writerTimezone,
      boolean skipProlepticConversion, boolean legacyConversionEnabled) throws IOException {
    this.descriptor = descriptor;
    this.type = type;
    this.hiveType = hiveType;
    this.skipTimestampConversion = skipTimestampConversion;
    this.writerTimezone = writerTimezone;
    this.skipProlepticConversion = skipProlepticConversion;
    this.legacyConversionEnabled = legacyConversionEnabled;
    try {
      this.dictionary = dictionaryPage == null ? null
          : ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(type, hiveType,
              dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage), skipTimestampConversion,
              writerTimezone, legacyConversionEnabled);
    } catch (IOException e) {
      throw new IOException("could not decode the dictionary for " + descriptor, e);
    }
  }

  /** The updater for the vector the column is read into; a column chunk keeps one vector type. */
  ParquetVectorUpdater create(ColumnVector column) throws IOException {
    ParquetVectorUpdater bulk = bulkUpdater(column);
    return bulk != null ? bulk : new PerValueUpdater(descriptor, type, hiveType, dictionaryValues(column),
        skipTimestampConversion, writerTimezone, skipProlepticConversion, legacyConversionEnabled);
  }

  /** The per-value updater for the pages the column's updater does not read in bulk; it shares the dictionary half. */
  PerValueUpdater perValue(ParquetVectorUpdater updater) {
    return updater instanceof PerValueUpdater perValue ? perValue : new PerValueUpdater(descriptor, type, hiveType,
        (DictionaryValues) updater, skipTimestampConversion, writerTimezone, skipProlepticConversion,
        legacyConversionEnabled);
  }

  /**
   * The pairings whose values need no conversion, which read PLAIN pages in bulk: signed integers into the integer
   * types and DATE, BOOLEAN, FLOAT and DOUBLE into their own types, decimals into decimal64 at their own scale, and
   * BINARY into the string types. Null for the rest, which read per value.
   */
  private ParquetVectorUpdater bulkUpdater(ColumnVector column) {
    PrimitiveCategory category = ((PrimitiveTypeInfo) hiveType).getPrimitiveCategory();
    boolean integer = !(type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation)
        && !ETypeConverter.isUnsignedInteger(type);
    boolean bulk = switch (type.getPrimitiveTypeName()) {
      case INT32 -> integer && (category == PrimitiveCategory.INT || category == PrimitiveCategory.LONG
          || category == PrimitiveCategory.INTERVAL_YEAR_MONTH || category == PrimitiveCategory.DATE);
      case INT64 -> integer
          && (category == PrimitiveCategory.LONG || category == PrimitiveCategory.INTERVAL_YEAR_MONTH);
      case BOOLEAN -> category == PrimitiveCategory.BOOLEAN;
      case FLOAT -> category == PrimitiveCategory.FLOAT;
      case DOUBLE -> category == PrimitiveCategory.DOUBLE;
      case BINARY -> !(type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation)
          && (category == PrimitiveCategory.STRING || category == PrimitiveCategory.CHAR
          || category == PrimitiveCategory.VARCHAR || category == PrimitiveCategory.BINARY);
      default -> false;
    };
    if (column instanceof Decimal64ColumnVector && sameScaleDecimal64()) {
      return new ParquetVectorUpdaters.Decimal64Updater(dictionary, type, hiveType);
    }
    if (!bulk) {
      return null;
    }
    if (type.getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
      // Only a UTF8 column truncates to a CHAR or VARCHAR length, as TypesFromStringPageReader does.
      int maxLength = type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation
          && hiveType instanceof BaseCharTypeInfo c ? c.getLength() : -1;
      return new ParquetVectorUpdaters.BinaryUpdater(dictionary, type, hiveType, maxLength);
    }
    return category == PrimitiveCategory.FLOAT || category == PrimitiveCategory.DOUBLE
        ? new ParquetVectorUpdaters.DoubleUpdater(dictionary, type, hiveType)
        : new ParquetVectorUpdaters.LongUpdater(dictionary, type, hiveType, skipProlepticConversion);
  }

  /**
   * A decimal stored at the Hive scale, as INT32, INT64 or a FIXED_LEN_BYTE_ARRAY of at most 18 digits, whose unscaled
   * value is the decimal64 value.
   */
  private boolean sameScaleDecimal64() {
    if (!(type.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation d)
        || d.getScale() != ((DecimalTypeInfo) hiveType).getScale()) {
      return false;
    }
    return switch (type.getPrimitiveTypeName()) {
      case INT32, INT64 -> true;
      case FIXED_LEN_BYTE_ARRAY -> HiveDecimalWritable.isPrecisionDecimal64(d.getPrecision());
      default -> false;
    };
  }

  private DictionaryValues dictionaryValues(ColumnVector column) throws IOException {
    return switch (((PrimitiveTypeInfo) hiveType).getPrimitiveCategory()) {
      case BOOLEAN, BYTE, SHORT, INT, LONG, INTERVAL_YEAR_MONTH, DATE ->
          new DictionaryValues.Longs(dictionary, type, hiveType, skipProlepticConversion);
      case FLOAT, DOUBLE -> new DictionaryValues.Doubles(dictionary, type, hiveType);
      case STRING, CHAR, VARCHAR, BINARY -> new DictionaryValues.Bytes(dictionary, type, hiveType);
      case DECIMAL -> column instanceof Decimal64ColumnVector
          ? new DictionaryValues.Decimal64s(dictionary, type, hiveType)
          : new DictionaryValues.Decimals(dictionary, type, hiveType);
      case TIMESTAMP -> new DictionaryValues.Timestamps(dictionary, type, hiveType);
      default -> throw new IOException("Unsupported type: " + type);
    };
  }
}
