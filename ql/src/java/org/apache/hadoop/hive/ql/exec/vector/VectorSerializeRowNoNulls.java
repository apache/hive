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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * This class serializes columns from a row in a VectorizedRowBatch into a serialization format.
 *
 * The caller provides the hive type names and column numbers in the order desired to
 * serialize.
 *
 * This class uses an provided SerializeWrite object to directly serialize by writing
 * field-by-field into a serialization format from the primitive values of the VectorizedRowBatch.
 *
 * Note that when serializing a row, the logical mapping using selected in use has already
 * been performed.
 *
 * NOTE: This class is a variation of VectorSerializeRow for serialization of columns that
 * have no nulls.
 */
public class VectorSerializeRowNoNulls {
  private static final Log LOG = LogFactory.getLog(VectorSerializeRowNoNulls.class.getName());

  private SerializeWrite serializeWrite;

  public VectorSerializeRowNoNulls(SerializeWrite serializeWrite) {
    this();
    this.serializeWrite = serializeWrite;
  }

  // Not public since we must have the serialize write object.
  private VectorSerializeRowNoNulls() {
  }

  private abstract class Writer {
    protected int columnIndex;

    Writer(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    abstract void apply(VectorizedRowBatch batch, int batchIndex) throws IOException;
  }

  private abstract class AbstractLongWriter extends Writer {

    AbstractLongWriter(int columnIndex) {
      super(columnIndex);
    }
  }

  private class BooleanWriter extends AbstractLongWriter {

    BooleanWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeBoolean(colVector.vector[colVector.isRepeating ? 0 : batchIndex] != 0);
    }
  }

  private class ByteWriter extends AbstractLongWriter {

	  ByteWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeByte((byte) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class ShortWriter extends AbstractLongWriter {

    ShortWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeShort((short) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class IntWriter extends AbstractLongWriter {

    IntWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeInt((int) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class LongWriter extends AbstractLongWriter {

    LongWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeLong(colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class DateWriter extends AbstractLongWriter {

    DateWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeDate((int) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class TimestampWriter extends AbstractLongWriter {

    Timestamp scratchTimestamp;

    TimestampWriter(int columnIndex) {
      super(columnIndex);
      scratchTimestamp =  new Timestamp(0);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      TimestampColumnVector colVector = (TimestampColumnVector) batch.cols[columnIndex];
      colVector.timestampUpdate(scratchTimestamp, colVector.isRepeating ? 0 : batchIndex);
      serializeWrite.writeTimestamp(scratchTimestamp);
    }
  }

  private class IntervalYearMonthWriter extends AbstractLongWriter {

    IntervalYearMonthWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      LongColumnVector colVector = (LongColumnVector) batch.cols[columnIndex];
      serializeWrite.writeHiveIntervalYearMonth((int) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class IntervalDayTimeWriter extends AbstractLongWriter {

    private HiveIntervalDayTime hiveIntervalDayTime;

    IntervalDayTimeWriter(int columnIndex) {
      super(columnIndex);
      hiveIntervalDayTime = new HiveIntervalDayTime();
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      IntervalDayTimeColumnVector colVector = (IntervalDayTimeColumnVector) batch.cols[columnIndex];
      hiveIntervalDayTime.set(colVector.asScratchIntervalDayTime(colVector.isRepeating ? 0 : batchIndex));
      serializeWrite.writeHiveIntervalDayTime(hiveIntervalDayTime);
    }
  }

  private abstract class AbstractDoubleWriter extends Writer {

    AbstractDoubleWriter(int columnIndex) {
      super(columnIndex);
    }
  }

  private class FloatWriter extends AbstractDoubleWriter {

    FloatWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DoubleColumnVector colVector = (DoubleColumnVector) batch.cols[columnIndex];
      serializeWrite.writeFloat((float) colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class DoubleWriter extends AbstractDoubleWriter {

    DoubleWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DoubleColumnVector colVector = (DoubleColumnVector) batch.cols[columnIndex];
      serializeWrite.writeDouble(colVector.vector[colVector.isRepeating ? 0 : batchIndex]);
    }
  }

  private class StringWriter extends Writer {

    StringWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (colVector.isRepeating) {
        serializeWrite.writeString(colVector.vector[0], colVector.start[0], colVector.length[0]);
      } else {
        serializeWrite.writeString(colVector.vector[batchIndex], colVector.start[batchIndex], colVector.length[batchIndex]);
      }
    }
  }

  private class BinaryWriter extends Writer {

    BinaryWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      BytesColumnVector colVector = (BytesColumnVector) batch.cols[columnIndex];

      if (colVector.isRepeating) {
        serializeWrite.writeBinary(colVector.vector[0], colVector.start[0], colVector.length[0]);
      } else {
        serializeWrite.writeBinary(colVector.vector[batchIndex], colVector.start[batchIndex], colVector.length[batchIndex]);
      }
    }
  }

  private class HiveDecimalWriter extends Writer {

    HiveDecimalWriter(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void apply(VectorizedRowBatch batch, int batchIndex) throws IOException {
      DecimalColumnVector colVector = (DecimalColumnVector) batch.cols[columnIndex];
      serializeWrite.writeHiveDecimal(colVector.vector[colVector.isRepeating ? 0 : batchIndex].getHiveDecimal());
    }
  }

  private Writer[] writers;

  private Writer createWriter(TypeInfo typeInfo, int columnIndex) throws HiveException {
    Writer writer;
    Category category = typeInfo.getCategory();
    switch (category) {
    case PRIMITIVE:
      {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        switch (primitiveCategory) {
        // case VOID:
        //   UNDONE:
        // break;
        case BOOLEAN:
          writer = new BooleanWriter(columnIndex);
          break;
        case BYTE:
          writer = new ByteWriter(columnIndex);
          break;
        case SHORT:
          writer = new ShortWriter(columnIndex);
          break;
        case INT:
          writer = new IntWriter(columnIndex);
          break;
        case LONG:
          writer = new LongWriter(columnIndex);
          break;
        case DATE:
          writer = new DateWriter(columnIndex);
          break;
        case TIMESTAMP:
          writer = new TimestampWriter(columnIndex);
          break;
        case FLOAT:
          writer = new FloatWriter(columnIndex);
          break;
        case DOUBLE:
          writer = new DoubleWriter(columnIndex);
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
          // We store CHAR and VARCHAR without pads, so use STRING writer class.
          writer = new StringWriter(columnIndex);
          break;
        case BINARY:
          writer = new BinaryWriter(columnIndex);
          break;
        case DECIMAL:
          writer = new HiveDecimalWriter(columnIndex);
          break;
        case INTERVAL_YEAR_MONTH:
          writer = new IntervalYearMonthWriter(columnIndex);
          break;
        case INTERVAL_DAY_TIME:
          writer = new IntervalDayTimeWriter(columnIndex);
          break;
        default:
          throw new HiveException("Unexpected primitive type category " + primitiveCategory);
        }
      }
      break;
    default:
      throw new HiveException("Unexpected type category " + category);
    }
    return writer;
  }

  public void init(List<String> typeNames, int[] columnMap) throws HiveException {

    writers = new Writer[typeNames.size()];
    for (int i = 0; i < typeNames.size(); i++) {
      String typeName = typeNames.get(i);
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      int columnIndex = columnMap[i];
      Writer writer = createWriter(typeInfo, columnIndex);
      writers[i] = writer;
    }
  }

  public void init(List<String> typeNames) throws HiveException {

    writers = new Writer[typeNames.size()];
    for (int i = 0; i < typeNames.size(); i++) {
      String typeName = typeNames.get(i);
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      Writer writer = createWriter(typeInfo, i);
      writers[i] = writer;
    }
  }

  public void init(PrimitiveTypeInfo[] primitiveTypeInfos, List<Integer> columnMap)
      throws HiveException {

    writers = new Writer[primitiveTypeInfos.length];
    for (int i = 0; i < primitiveTypeInfos.length; i++) {
      int columnIndex = columnMap.get(i);
      Writer writer = createWriter(primitiveTypeInfos[i], columnIndex);
      writers[i] = writer;
    }
  }

  public int getCount() {
    return writers.length;
  }

  public void setOutput(Output output) {
    serializeWrite.set(output);
  }

  public void setOutputAppend(Output output) {
    serializeWrite.setAppend(output);
  }

  /*
   * Note that when serializing a row, the logical mapping using selected in use has already
   * been performed.  batchIndex is the actual index of the row.
   */
  public void serializeWriteNoNulls(VectorizedRowBatch batch, int batchIndex) throws IOException {
    for (Writer writer : writers) {
      writer.apply(batch, batchIndex);
    }
  }
}