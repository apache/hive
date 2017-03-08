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

import com.google.common.base.Strings;
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
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/**
 * It's column level Parquet reader which is used to read a batch of records for a column,
 * part of the code is referred from Apache Spark and Apache Parquet.
 */
public class VectorizedPrimitiveColumnReader implements VectorizedColumnReader {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedPrimitiveColumnReader.class);

  private String conversionTimeZone;

  /**
   * Total number of values read.
   */
  private long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * Maximum definition level for this column.
   */
  private final int maxDefLevel;

  private int definitionLevel;
  private int repetitionLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private IntIterator repetitionLevelColumn;
  private IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final Type type;

  public VectorizedPrimitiveColumnReader(
    ColumnDescriptor descriptor,
    PageReader pageReader,
    String conversionTimeZone,
    Type type) throws IOException {
    this.descriptor = descriptor;
    this.type = type;
    this.pageReader = pageReader;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();
    this.conversionTimeZone = conversionTimeZone;

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
  }

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
      final Calendar calendar;
      if (Strings.isNullOrEmpty(this.conversionTimeZone)) {
        // Local time should be used if no timezone is specified
        calendar = Calendar.getInstance();
      } else {
        calendar = Calendar.getInstance(TimeZone.getTimeZone(this.conversionTimeZone));
      }
      for (int i = rowId; i < rowId + num; ++i) {
        ByteBuffer buf = dictionary.decodeToBinary((int) dictionaryIds.vector[i]).toByteBuffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = buf.getLong();
        int julianDay = buf.getInt();
        NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
        Timestamp ts = NanoTimeUtils.getTimestamp(nt, calendar);
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

  private void readRepetitionAndDefinitionLevels() {
    repetitionLevel = repetitionLevelColumn.nextInt();
    definitionLevel = definitionLevelColumn.nextInt();
    valuesRead++;
  }

  private void readPage() throws IOException {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        readPageV1(dataPageV1);
        return null;
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        readPageV2(dataPageV2);
        return null;
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset, int valueCount) throws IOException {
    this.pageValueCount = valueCount;
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
          "could not read page in col " + descriptor +
            " as the dictionary was missing for encoding " + dataEncoding);
      }
      dataColumn = dataEncoding.getDictionaryBasedValuesReader(descriptor, VALUES, dictionary);
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      dataColumn = dataEncoding.getValuesReader(descriptor, VALUES);
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) {
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      LOG.debug("page size " + bytes.length + " bytes and " + pageValueCount + " records");
      LOG.debug("reading repetition levels at 0");
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      LOG.debug("reading definition levels at " + next);
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      LOG.debug("reading data at " + next);
      initDataReader(page.getValueEncoding(), bytes, next, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = newRLEIterator(descriptor.getMaxRepetitionLevel(),
      page.getRepetitionLevels());
    this.definitionLevelColumn = newRLEIterator(descriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
    try {
      LOG.debug("page data size " + page.getData().size() + " bytes and " + pageValueCount + " records");
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0, page.getValueCount());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new NullIntIterator();
      }
      return new RLEIntIterator(
        new RunLengthBitPackingHybridDecoder(
          BytesUtils.getWidthFromMaxInt(maxLevel),
          new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read levels in page for col " + descriptor, e);
    }
  }

  /**
   * Utility classes to abstract over different way to read ints with different encodings.
   * TODO: remove this layer of abstraction?
   */
  abstract static class IntIterator {
    abstract int nextInt();
  }

  protected static final class ValuesReaderIntIterator extends IntIterator {
    ValuesReader delegate;

    public ValuesReaderIntIterator(ValuesReader delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      return delegate.readInteger();
    }
  }

  protected static final class RLEIntIterator extends IntIterator {
    RunLengthBitPackingHybridDecoder delegate;

    public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
      this.delegate = delegate;
    }

    @Override
    int nextInt() {
      try {
        return delegate.readInt();
      } catch (IOException e) {
        throw new ParquetDecodingException(e);
      }
    }
  }

  protected static final class NullIntIterator extends IntIterator {
    @Override
    int nextInt() { return 0; }
  }
}
