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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/**
 * Reads a primitive column with no repetition, a top-level column or a struct field, a batch at a time: a page's
 * definition levels decode first, straight into the vector's NULL flags, then the updater the
 * {@link ParquetVectorUpdaterFactory} picked for the column fills the non-NULL rows, from dictionary ids or from the
 * page's values.
 */
public class VectorizedPrimitiveColumnReader implements VectorizedColumnReader {

  private final ColumnDescriptor descriptor;
  private final PageReader pageReader;
  private final int maxDefLevel;
  private final boolean hasDictionary;
  private final boolean structField;
  private final ParquetVectorUpdaterFactory updaterFactory;
  /** Definition levels, null when maxDefLevel is 0. */
  private final VectorizedRleValuesReader levels;
  /** The definition levels of a page in an encoding other than RLE, or null. */
  private ValuesReader levelValues;
  private final VectorizedRleValuesReader dictionaryIds = new VectorizedRleValuesReader(0);
  private final VectorizedPlainValuesReader plainValues = new VectorizedPlainValuesReader();
  private ParquetVectorUpdater updater;
  /** Reads the pages the updater does not read in bulk. */
  private PerValueUpdater perValue;
  private ParquetVectorUpdater pageUpdater;
  private int[] ids = new int[0];
  /** A struct field's definition levels, where the struct reader finds its own NULLs. */
  private int[] definitionLevels;
  private int valuesLeftInPage;
  private boolean dictionaryPage;

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
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();
    this.levels = maxDefLevel == 0 ? null : new VectorizedRleValuesReader(BytesUtils.getWidthFromMaxInt(maxDefLevel));
    this.structField = descriptor.getPath().length > 1;
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    this.hasDictionary = dictionaryPage != null;
    this.updaterFactory = new ParquetVectorUpdaterFactory(descriptor, type.asPrimitiveType(), hiveType,
        dictionaryPage, skipTimestampConversion, writerTimezone, skipProlepticConversion, legacyConversionEnabled);
  }

  @Override
  public void readBatch(int total, ColumnVector column, TypeInfo columnType) throws IOException {
    if (total == 0) {
      return;
    }
    beginBatch(total, column);
    boolean dictionaryOnly = true;
    int repeatedId = -1;
    for (int rowId = 0; rowId < total; ) {
      if (valuesLeftInPage == 0) {
        readNextPage();
      }
      int n = Math.min(total - rowId, valuesLeftInPage);
      int nonNull = n - readLevels(n, column, rowId);
      if (dictionaryPage) {
        int id = dictionaryIds.readDictionaryIds(nonNull, ids);
        repeatedId = rowId == 0 || id == repeatedId ? id : -1;
        updater.decodeDictionaryIds(n, nonNull, rowId, column, ids);
      } else {
        dictionaryOnly = false;
        pageUpdater.readValues(n, nonNull, rowId, column, plainValues);
      }
      valuesLeftInPage -= n;
      rowId += n;
    }
    // A NULL makes a batch non-repeating, as does a caller that did not allow it (a struct field).
    column.isRepeating = column.isRepeating && column.noNulls
        && updater.isRepeating(column, total, dictionaryOnly, repeatedId >= 0);
  }

  /** Picks the updater at the first batch, once the vector type is known, and sizes the arrays of a batch. */
  private void beginBatch(int total, ColumnVector column) throws IOException {
    if (updater == null) {
      updater = updaterFactory.create(column);
      perValue = updaterFactory.perValue(updater);
    }
    if (ids.length < total) {
      ids = new int[total];
      definitionLevels = structField ? new int[total] : null;
    }
    updater.beginBatch(column);
  }

  /**
   * The definition levels of the next {@code n} rows into the NULL flags and, for a struct field, into
   * {@link #definitionLevels}; returns how many rows are NULL. A field with no definition level stays at level 0.
   */
  private int readLevels(int n, ColumnVector column, int rowId) {
    if (levels == null) {
      Arrays.fill(column.isNull, rowId, rowId + n, false);
      return 0;
    }
    int nulls = levelValues == null
        ? levels.readDefinitionLevels(n, maxDefLevel, column.isNull, definitionLevels, rowId)
        : readLevelsPerValue(n, column.isNull, rowId);
    column.noNulls &= nulls == 0;
    return nulls;
  }

  /** Levels in an encoding other than RLE, BIT_PACKED from writers before parquet-mr 1.0, one at a time. */
  private int readLevelsPerValue(int n, boolean[] isNull, int rowId) {
    int nulls = 0;
    for (int i = rowId; i < rowId + n; i++) {
      int level = levelValues.readInteger();
      isNull[i] = level < maxDefLevel;
      nulls += isNull[i] ? 1 : 0;
      if (definitionLevels != null) {
        definitionLevels[i] = level;
      }
    }
    return nulls;
  }

  private void readNextPage() throws IOException {
    DataPage page = pageReader.readPage();
    valuesLeftInPage = page.getValueCount();
    try {
      levelValues = null;
      if (page instanceof DataPageV1 v1) {
        ByteBufferInputStream in = v1.getBytes().toInputStream();
        if (levels != null && v1.getDlEncoding() == Encoding.RLE) {
          levels.initFromPage(in.slice(BytesUtils.readIntLittleEndian(in)));
        } else if (levels != null) {
          levelValues = v1.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
          levelValues.initFromPage(valuesLeftInPage, in);
        }
        initValues(v1.getValueEncoding(), in);
      } else {
        DataPageV2 v2 = (DataPageV2) page;
        if (levels != null) {
          levels.initFromPage(v2.getDefinitionLevels().toByteBuffer());
        }
        initValues(v2.getDataEncoding(), v2.getData().toInputStream());
      }
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  /**
   * Sets up the values of a page: dictionary ids; the page buffer itself for a PLAIN page the updater reads in bulk;
   * otherwise parquet-mr's reader for the encoding, read per value.
   */
  private void initValues(Encoding encoding, ByteBufferInputStream in) throws IOException {
    dictionaryPage = encoding.usesDictionary();
    if (dictionaryPage) {
      if (!hasDictionary) {
        throw new IOException("the dictionary was missing for encoding " + encoding);
      }
      dictionaryIds.initDictionaryIds(in.slice(in.available()));
    } else if (encoding == Encoding.PLAIN && updater.readsPlainInBulk()) {
      plainValues.initFromPage(in.slice(in.available()));
      pageUpdater = updater;
    } else {
      ValuesReader values = encoding.getValuesReader(descriptor, VALUES);
      values.initFromPage(valuesLeftInPage, in);
      perValue.startPage(values);
      pageUpdater = perValue;
    }
  }

  @Override
  public int[] getDefinitionLevels() {
    return definitionLevels;
  }
}
