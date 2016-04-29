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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.common.util.DateUtils;

/**
 * This class deserializes a serialization format into a row of a VectorizedRowBatch.
 * 
 * The caller provides the hive type names and output column numbers in the order desired to
 * deserialize.
 *
 * This class uses an provided DeserializeRead object to directly deserialize by reading
 * field-by-field from a serialization format into the primitive values of the VectorizedRowBatch.
 */

public class VectorDeserializeRow {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorDeserializeRow.class);

  private DeserializeRead deserializeRead;

  private Reader[] readersByValue;
  private Reader[] readersByReference;
  private TypeInfo[] typeInfos;

  public VectorDeserializeRow(DeserializeRead deserializeRead) {
    this();
    this.deserializeRead = deserializeRead;
    typeInfos = deserializeRead.typeInfos();
    
  }

  // Not public since we must have the deserialize read object.
  private VectorDeserializeRow() {
  }

  private abstract class Reader {
    protected int columnIndex;

    Reader(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    abstract void apply(VectorizedRowBatch batch, int batchIndex) throws IOException;
  }

  private abstract class AbstractLongReader extends Reader {

    AbstractLongReader(int columnIndex) {
      super(columnIndex);
    }
  }

  private class BooleanReader extends AbstractLongReader {

    BooleanReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        boolean value = deserializeRead.readBoolean();
        colVector.vector[batchIndex] = (value ? 1 : 0);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class ByteReader extends AbstractLongReader {

    ByteReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        byte value = deserializeRead.readByte();
        colVector.vector[batchIndex] = (long) value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class ShortReader extends AbstractLongReader {

    ShortReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        short value = deserializeRead.readShort();
        colVector.vector[batchIndex] = (long) value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class IntReader extends AbstractLongReader {

    IntReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        int value = deserializeRead.readInt();
        colVector.vector[batchIndex] = (long) value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class LongReader extends AbstractLongReader {

    LongReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        long value = deserializeRead.readLong();
        colVector.vector[batchIndex] = value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class DateReader extends AbstractLongReader {

    DeserializeRead.ReadDateResults readDateResults;

    DateReader(int columnIndex) {
      super(columnIndex);
      readDateResults = deserializeRead.createReadDateResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readDate(readDateResults);
        colVector.vector[batchIndex] = (long) readDateResults.getDays();
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private abstract class AbstractTimestampReader extends Reader {

    AbstractTimestampReader(int columnIndex) {
      super(columnIndex);
    }
  }

  private class TimestampReader extends AbstractTimestampReader {

    DeserializeRead.ReadTimestampResults readTimestampResults;

    TimestampReader(int columnIndex) {
      super(columnIndex);
      readTimestampResults = deserializeRead.createReadTimestampResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      TimestampColumnVector colVector = (TimestampColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readTimestamp(readTimestampResults);
        colVector.set(batchIndex, readTimestampResults.getTimestamp());
        colVector.isNull[batchIndex] = false;
      }
    }

  }

  private class IntervalYearMonthReader extends AbstractLongReader {

    DeserializeRead.ReadIntervalYearMonthResults readIntervalYearMonthResults;

    IntervalYearMonthReader(int columnIndex) {
      super(columnIndex);
      readIntervalYearMonthResults = deserializeRead.createReadIntervalYearMonthResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readIntervalYearMonth(readIntervalYearMonthResults);
        HiveIntervalYearMonth hiym = readIntervalYearMonthResults.getHiveIntervalYearMonth();
        colVector.vector[batchIndex] = hiym.getTotalMonths();
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private abstract class AbstractIntervalDayTimeReader extends Reader {

    AbstractIntervalDayTimeReader(int columnIndex) {
      super(columnIndex);
    }
  }

  private class IntervalDayTimeReader extends AbstractIntervalDayTimeReader {

    DeserializeRead.ReadIntervalDayTimeResults readIntervalDayTimeResults;

    IntervalDayTimeReader(int columnIndex) {
      super(columnIndex);
      readIntervalDayTimeResults = deserializeRead.createReadIntervalDayTimeResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      IntervalDayTimeColumnVector colVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readIntervalDayTime(readIntervalDayTimeResults);
        HiveIntervalDayTime idt = readIntervalDayTimeResults.getHiveIntervalDayTime();
        colVector.set(batchIndex, idt);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private abstract class AbstractDoubleReader extends Reader {

    AbstractDoubleReader(int columnIndex) {
      super(columnIndex);
    }
  }

  private class FloatReader extends AbstractDoubleReader {

    FloatReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DoubleColumnVector colVector = (DoubleColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        float value = deserializeRead.readFloat();
        colVector.vector[batchIndex] = (double) value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class DoubleReader extends AbstractDoubleReader {

    DoubleReader(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DoubleColumnVector colVector = (DoubleColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        double value = deserializeRead.readDouble();
        colVector.vector[batchIndex] = value;
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private abstract class AbstractBytesReader extends Reader {

    AbstractBytesReader(int columnIndex) {
      super(columnIndex);
    }
  }

  private class StringReaderByValue extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    StringReaderByValue(int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readString(readStringResults);
        colVector.setVal(batchIndex, readStringResults.bytes,
                readStringResults.start, readStringResults.length);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class StringReaderByReference extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    StringReaderByReference(int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readString(readStringResults);
        colVector.setRef(batchIndex, readStringResults.bytes,
                readStringResults.start, readStringResults.length);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class CharReaderByValue extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    private CharTypeInfo charTypeInfo;

    CharReaderByValue(CharTypeInfo charTypeInfo, int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
      this.charTypeInfo = charTypeInfo;
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        deserializeRead.readString(readStringResults);
        int adjustedLength = StringExpr.rightTrimAndTruncate(readStringResults.bytes,
                readStringResults.start, readStringResults.length, charTypeInfo.getLength());
        colVector.setVal(batchIndex, readStringResults.bytes, readStringResults.start, adjustedLength);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class CharReaderByReference extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    private CharTypeInfo charTypeInfo;

    CharReaderByReference(CharTypeInfo charTypeInfo, int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
      this.charTypeInfo = charTypeInfo;
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        deserializeRead.readString(readStringResults);
        int adjustedLength = StringExpr.rightTrimAndTruncate(readStringResults.bytes,
                readStringResults.start, readStringResults.length, charTypeInfo.getLength());
        colVector.setRef(batchIndex, readStringResults.bytes, readStringResults.start, adjustedLength);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class VarcharReaderByValue extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    private VarcharTypeInfo varcharTypeInfo;

    VarcharReaderByValue(VarcharTypeInfo varcharTypeInfo, int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
      this.varcharTypeInfo = varcharTypeInfo;
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        deserializeRead.readString(readStringResults);
        int adjustedLength = StringExpr.truncate(readStringResults.bytes,
                readStringResults.start, readStringResults.length, varcharTypeInfo.getLength());
        colVector.setVal(batchIndex, readStringResults.bytes, readStringResults.start, adjustedLength);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class VarcharReaderByReference extends AbstractBytesReader {

    private DeserializeRead.ReadStringResults readStringResults;

    private VarcharTypeInfo varcharTypeInfo;

    VarcharReaderByReference(VarcharTypeInfo varcharTypeInfo, int columnIndex) {
      super(columnIndex);
      readStringResults = deserializeRead.createReadStringResults();
      this.varcharTypeInfo = varcharTypeInfo;
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // Use the basic STRING bytes read to get access, then use our optimal truncate/trim method
        // that does not use Java String objects.
        deserializeRead.readString(readStringResults);
        int adjustedLength = StringExpr.truncate(readStringResults.bytes,
                readStringResults.start, readStringResults.length, varcharTypeInfo.getLength());
        colVector.setRef(batchIndex, readStringResults.bytes, readStringResults.start, adjustedLength);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class BinaryReaderByValue extends AbstractBytesReader {

    private DeserializeRead.ReadBinaryResults readBinaryResults;

    BinaryReaderByValue(int columnIndex) {
      super(columnIndex);
      readBinaryResults = deserializeRead.createReadBinaryResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readBinary(readBinaryResults);
        colVector.setVal(batchIndex, readBinaryResults.bytes,
                readBinaryResults.start, readBinaryResults.length);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class BinaryReaderByReference extends AbstractBytesReader {

    private DeserializeRead.ReadBinaryResults readBinaryResults;

    BinaryReaderByReference(int columnIndex) {
      super(columnIndex);
      readBinaryResults = deserializeRead.createReadBinaryResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readBinary(readBinaryResults);
        colVector.setRef(batchIndex, readBinaryResults.bytes,
                readBinaryResults.start, readBinaryResults.length);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private class HiveDecimalReader extends Reader {

    private DeserializeRead.ReadDecimalResults readDecimalResults;

    HiveDecimalReader(int columnIndex) {
      super(columnIndex);
      readDecimalResults = deserializeRead.createReadDecimalResults();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DecimalColumnVector colVector = (DecimalColumnVector) batch.cols[columnIndex];

      if (deserializeRead.readCheckNull()) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        deserializeRead.readHiveDecimal(readDecimalResults);
        HiveDecimal hiveDecimal = readDecimalResults.getHiveDecimal();
        colVector.vector[batchIndex].set(hiveDecimal);
        colVector.isNull[batchIndex] = false;
      }
    }
  }

  private void addReader(int index, int outputColumn) throws HiveException {
    Reader readerByValue = null;
    Reader readerByReference = null;

    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfos[index];
    PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
    switch (primitiveCategory) {
    // case VOID:
    //   UNDONE:
    // break;
    case BOOLEAN:
      readerByValue = new BooleanReader(outputColumn);
      break;
    case BYTE:
      readerByValue = new ByteReader(outputColumn);
      break;
    case SHORT:
      readerByValue = new ShortReader(outputColumn);
      break;
    case INT:
      readerByValue = new IntReader(outputColumn);
      break;
    case LONG:
      readerByValue = new LongReader(outputColumn);
      break;
    case DATE:
      readerByValue = new DateReader(outputColumn);
      break;
    case TIMESTAMP:
      readerByValue = new TimestampReader(outputColumn);
      break;
    case FLOAT:
      readerByValue = new FloatReader(outputColumn);
      break;
    case DOUBLE:
      readerByValue = new DoubleReader(outputColumn);
      break;
    case STRING:
      readerByValue = new StringReaderByValue(outputColumn);
      readerByReference = new StringReaderByReference(outputColumn);
      break;
    case CHAR:
      {
        CharTypeInfo charTypeInfo = (CharTypeInfo) primitiveTypeInfo;
        readerByValue = new CharReaderByValue(charTypeInfo, outputColumn);
        readerByReference = new CharReaderByReference(charTypeInfo, outputColumn);
      }
      break;
    case VARCHAR:
      {
        VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) primitiveTypeInfo;
        readerByValue = new VarcharReaderByValue(varcharTypeInfo, outputColumn);
        readerByReference = new VarcharReaderByReference(varcharTypeInfo, outputColumn);
      }
      break;
    case BINARY:
      readerByValue = new BinaryReaderByValue(outputColumn);
      readerByReference = new BinaryReaderByReference(outputColumn);
      break;
    case DECIMAL:
      readerByValue = new HiveDecimalReader(outputColumn);
      break;
    case INTERVAL_YEAR_MONTH:
      readerByValue = new IntervalYearMonthReader(outputColumn);
      break;
    case INTERVAL_DAY_TIME:
      readerByValue = new IntervalDayTimeReader(outputColumn);
      break;
    default:
      throw new HiveException("Unexpected primitive type category " + primitiveCategory);
    }

    readersByValue[index] = readerByValue;
    if (readerByReference == null) {
      readersByReference[index] = readerByValue;
    } else {
      readersByReference[index] = readerByReference;
    }
  }

  public void init(int[] outputColumns) throws HiveException {

    readersByValue = new Reader[typeInfos.length];
    readersByReference = new Reader[typeInfos.length];

    for (int i = 0; i < typeInfos.length; i++) {
      int outputColumn = outputColumns[i];
      addReader(i, outputColumn);
    }
  }

  public void init(List<Integer> outputColumns) throws HiveException {

    readersByValue = new Reader[typeInfos.length];
    readersByReference = new Reader[typeInfos.length];

    for (int i = 0; i < typeInfos.length; i++) {
      int outputColumn = outputColumns.get(i);
      addReader(i, outputColumn);
    }
  }

  public void init(int startColumn) throws HiveException {

    readersByValue = new Reader[typeInfos.length];
    readersByReference = new Reader[typeInfos.length];

    for (int i = 0; i < typeInfos.length; i++) {
      int outputColumn = startColumn + i;
      addReader(i, outputColumn);
    }
  }

  public void init() throws HiveException {
    init(0);
  }

  public void setBytes(byte[] bytes, int offset, int length) {
    deserializeRead.set(bytes, offset, length);
  }

  public void deserializeByValue(VectorizedRowBatch batch, int batchIndex) throws IOException {
    int i = 0;
    try {
      while (i < readersByValue.length) {
        readersByValue[i].apply(batch, batchIndex);
        i++;    // Increment after the apply which could throw an exception.
      }
    } catch (EOFException e) {
      throwMoreDetailedException(e, i);
    }
    deserializeRead.extraFieldsCheck();
  }

  public void deserializeByReference(VectorizedRowBatch batch, int batchIndex) throws IOException {
    int i = 0;
    try {
      while (i < readersByReference.length) {
      readersByReference[i].apply(batch, batchIndex);
      i++;    // Increment after the apply which could throw an exception.
    }
    } catch (EOFException e) {
      throwMoreDetailedException(e, i);
    }
    deserializeRead.extraFieldsCheck();
  }

  private void throwMoreDetailedException(IOException e, int index) throws EOFException {
    StringBuilder sb = new StringBuilder();
    sb.append("Detail: \"" + e.toString() + "\" occured for field " + index + " of " +  typeInfos.length + " fields (");
    for (int i = 0; i < typeInfos.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(((PrimitiveTypeInfo) typeInfos[i]).getPrimitiveCategory().name());
    }
    sb.append(")");
    throw new EOFException(sb.toString());
  }
}
