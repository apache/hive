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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

/**
 * This class extracts specified VectorizedRowBatch row columns into a Writable row Object[].
 *
 * The caller provides the hive type names and target column numbers in the order desired to
 * extract from the Writable row Object[].
 *
 * This class is abstract to allow the subclasses to control batch reuse.
 */
public abstract class VectorExtractRow {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorExtractRow.class);

  private boolean tolerateNullColumns;

  public VectorExtractRow() {
    // UNDONE: For now allow null columns until vector_decimal_mapjoin.q is understood...
    tolerateNullColumns = true;
  }

  protected abstract class Extractor {
    protected int columnIndex;
    protected Object object;

    public Extractor(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    public int getColumnIndex() {
      return columnIndex;
    }

    abstract void setColumnVector(VectorizedRowBatch batch);

    abstract void forgetColumnVector();

    abstract Object extract(int batchIndex);
  }

  private class VoidExtractor extends Extractor {

    VoidExtractor(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
    }

    @Override
    void forgetColumnVector() {
    }

    @Override
    Object extract(int batchIndex) {
      return null;
    }
  }

  private abstract class AbstractLongExtractor extends Extractor {

    protected LongColumnVector colVector;
    protected long[] vector;

    AbstractLongExtractor(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
      colVector = (LongColumnVector) batch.cols[columnIndex];
      vector = colVector.vector;
    }

    @Override
    void forgetColumnVector() {
      colVector = null;
      vector = null;
    }
  }

  protected class BooleanExtractor extends AbstractLongExtractor {

    BooleanExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.create(false);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.set(object, value == 0 ? false : true);
        return object;
      } else {
        return null;
      }
    }
  }

  protected class ByteExtractor extends AbstractLongExtractor {

    ByteExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableByteObjectInspector.create((byte) 0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableByteObjectInspector.set(object, (byte) value);
        return object;
      } else {
        return null;
      }
    }
  }

  private class ShortExtractor extends AbstractLongExtractor {

    ShortExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableShortObjectInspector.create((short) 0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableShortObjectInspector.set(object, (short) value);
        return object;
      } else {
        return null;
      }
    }
  }

  private class IntExtractor extends AbstractLongExtractor {

    IntExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableIntObjectInspector.create(0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableIntObjectInspector.set(object, (int) value);
        return object;
      } else {
        return null;
      }
    }
  }

  private class LongExtractor extends AbstractLongExtractor {

    LongExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableLongObjectInspector.create(0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableLongObjectInspector.set(object, value);
        return object;
      } else {
        return null;
      }
    }
  }

  private class DateExtractor extends AbstractLongExtractor {

    private Date date;

    DateExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableDateObjectInspector.create(new Date(0));
      date = new Date(0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        date.setTime(DateWritable.daysToMillis((int) value));
        PrimitiveObjectInspectorFactory.writableDateObjectInspector.set(object, date);
        return object;
      } else {
        return null;
      }
    }
  }

  private class TimestampExtractor extends AbstractLongExtractor {

    private Timestamp timestamp;
    
    TimestampExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector.create(new Timestamp(0));
      timestamp = new Timestamp(0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        TimestampUtils.assignTimeInNanoSec(value, timestamp);
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector.set(object, timestamp);
        return object;
      } else {
        return null;
      }
    }
  }

  private class IntervalYearMonthExtractor extends AbstractLongExtractor {

    private HiveIntervalYearMonth hiveIntervalYearMonth;
    
    IntervalYearMonthExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector.create(new HiveIntervalYearMonth(0));
      hiveIntervalYearMonth = new HiveIntervalYearMonth(0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        int totalMonths = (int) vector[adjustedIndex];
        hiveIntervalYearMonth.set(totalMonths);
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector.set(object, hiveIntervalYearMonth);
        return object;
      } else {
        return null;
      }
    }
  }

  private class IntervalDayTimeExtractor extends AbstractLongExtractor {

    private HiveIntervalDayTime hiveIntervalDayTime;
    
    IntervalDayTimeExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector.create(new HiveIntervalDayTime(0, 0));
      hiveIntervalDayTime = new HiveIntervalDayTime(0, 0);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        long value = vector[adjustedIndex];
        DateUtils.setIntervalDayTimeTotalNanos(hiveIntervalDayTime, value);
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector.set(object, hiveIntervalDayTime);
        return object;
      } else {
        return null;
      }
    }
  }

  private abstract class AbstractDoubleExtractor extends Extractor {

    protected DoubleColumnVector colVector;
    protected double[] vector;

    AbstractDoubleExtractor(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
      colVector = (DoubleColumnVector) batch.cols[columnIndex];
      vector = colVector.vector;
    }

    @Override
    void forgetColumnVector() {
      colVector = null;
      vector = null;
    }
  }

  private class FloatExtractor extends AbstractDoubleExtractor {

    FloatExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableFloatObjectInspector.create(0f);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        double value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector.set(object, (float) value);
        return object;
      } else {
        return null;
      }
    }
  }

  private class DoubleExtractor extends AbstractDoubleExtractor {

    DoubleExtractor(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.create(0f);
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        double value = vector[adjustedIndex];
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.set(object, value);
        return object;
      } else {
        return null;
      }
    }
  }

  private abstract class AbstractBytesExtractor extends Extractor {

    protected BytesColumnVector colVector;

    AbstractBytesExtractor(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
      colVector = (BytesColumnVector) batch.cols[columnIndex];
    }

    @Override
    void forgetColumnVector() {
      colVector = null;
    }
  }

  private class BinaryExtractorByValue extends AbstractBytesExtractor {

    private DataOutputBuffer buffer;

    // Use the BytesWritable instance here as a reference to data saved in buffer.  We do not
    // want to pass the binary object inspector a byte[] since we would need to allocate it on the
    // heap each time to get the length correct.
    private BytesWritable bytesWritable;

    BinaryExtractorByValue(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create(ArrayUtils.EMPTY_BYTE_ARRAY);
      buffer = new DataOutputBuffer();
      bytesWritable = new BytesWritable();
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        byte[] bytes = colVector.vector[adjustedIndex];
        int start = colVector.start[adjustedIndex];
        int length = colVector.length[adjustedIndex];

        // Save a copy of the binary data.
        buffer.reset();
        try {
          buffer.write(bytes, start, length);
        } catch (IOException ioe) {
          throw new IllegalStateException("bad write", ioe);
        }

        bytesWritable.set(buffer.getData(), 0, buffer.getLength());
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.set(object, bytesWritable);
        return object;
      } else {
        return null;
      }
    }
  }

  private class StringExtractorByValue extends AbstractBytesExtractor {

    // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
    private Text text;

    StringExtractorByValue(int columnIndex) {
      super(columnIndex);
      object = PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(StringUtils.EMPTY);
      text = new Text();
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        byte[] value = colVector.vector[adjustedIndex];
        int start = colVector.start[adjustedIndex];
        int length = colVector.length[adjustedIndex];

        // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
        text.set(value, start, length);

        PrimitiveObjectInspectorFactory.writableStringObjectInspector.set(object, text);
        return object;
      } else {
        return null;
      }
    }
  }

  private class VarCharExtractorByValue extends AbstractBytesExtractor {

    // We need our own instance of the VARCHAR object inspector to hold the maximum length
    // from the TypeInfo.
    private WritableHiveVarcharObjectInspector writableVarcharObjectInspector;

    // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
    private Text text;

    /*
     * @param varcharTypeInfo
     *                      We need the VARCHAR type information that contains the maximum length.
     * @param columnIndex
     *                      The vector row batch column that contains the bytes for the VARCHAR.
     */
    VarCharExtractorByValue(VarcharTypeInfo varcharTypeInfo, int columnIndex) {
      super(columnIndex);
      writableVarcharObjectInspector = new WritableHiveVarcharObjectInspector(varcharTypeInfo);
      object = writableVarcharObjectInspector.create(new HiveVarchar(StringUtils.EMPTY, -1));
      text = new Text();
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        byte[] value = colVector.vector[adjustedIndex];
        int start = colVector.start[adjustedIndex];
        int length = colVector.length[adjustedIndex];

        // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
        text.set(value, start, length);

        writableVarcharObjectInspector.set(object, text.toString());
        return object;
      } else {
        return null;
      }
    }
  }

  private class CharExtractorByValue extends AbstractBytesExtractor {

    // We need our own instance of the CHAR object inspector to hold the maximum length
    // from the TypeInfo.
    private WritableHiveCharObjectInspector writableCharObjectInspector;

    // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
    private Text text;

    /*
     * @param varcharTypeInfo
     *                      We need the CHAR type information that contains the maximum length.
     * @param columnIndex
     *                      The vector row batch column that contains the bytes for the CHAR.
     */
    CharExtractorByValue(CharTypeInfo charTypeInfo, int columnIndex) {
      super(columnIndex);
      writableCharObjectInspector = new WritableHiveCharObjectInspector(charTypeInfo);
      object = writableCharObjectInspector.create(new HiveChar(StringUtils.EMPTY, -1));
      text = new Text();
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        byte[] value = colVector.vector[adjustedIndex];
        int start = colVector.start[adjustedIndex];
        int length = colVector.length[adjustedIndex];

        // Use org.apache.hadoop.io.Text as our helper to go from byte[] to String.
        text.set(value, start, length);

        writableCharObjectInspector.set(object, text.toString());
        return object;
      } else {
        return null;
      }
    }
  }

  private class DecimalExtractor extends Extractor {

    private WritableHiveDecimalObjectInspector writableDecimalObjectInspector;
    protected DecimalColumnVector colVector;

    /*
     * @param decimalTypeInfo
     *                      We need the DECIMAL type information that contains scale and precision.
     * @param columnIndex
     *                      The vector row batch column that contains the bytes for the VARCHAR.
     */
    DecimalExtractor(DecimalTypeInfo decimalTypeInfo, int columnIndex) {
      super(columnIndex);
      writableDecimalObjectInspector = new WritableHiveDecimalObjectInspector(decimalTypeInfo);
      object = writableDecimalObjectInspector.create(HiveDecimal.ZERO);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
      colVector = (DecimalColumnVector) batch.cols[columnIndex];
    }

    @Override
    void forgetColumnVector() {
      colVector = null;
    }

    @Override
    Object extract(int batchIndex) {
      int adjustedIndex = (colVector.isRepeating ? 0 : batchIndex);
      if (colVector.noNulls || !colVector.isNull[adjustedIndex]) {
        HiveDecimal value = colVector.vector[adjustedIndex].getHiveDecimal();
        writableDecimalObjectInspector.set(object, value);
        return object;
      } else {
        return null;
      }
    }
  }

  private Extractor createExtractor(PrimitiveTypeInfo primitiveTypeInfo, int columnIndex) throws HiveException {
    PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
    Extractor extracter;
    switch (primitiveCategory) {
    case VOID:
      extracter = new VoidExtractor(columnIndex);
      break;
    case BOOLEAN:
      extracter = new BooleanExtractor(columnIndex);
      break;
    case BYTE:
      extracter = new ByteExtractor(columnIndex);
      break;
    case SHORT:
      extracter = new ShortExtractor(columnIndex);
      break;
    case INT:
      extracter = new IntExtractor(columnIndex);
      break;
    case LONG:
      extracter = new LongExtractor(columnIndex);
      break;
    case TIMESTAMP:
      extracter = new TimestampExtractor(columnIndex);
      break;
    case DATE:
      extracter = new DateExtractor(columnIndex);
      break;
    case FLOAT:
      extracter = new FloatExtractor(columnIndex);
      break;
    case DOUBLE:
      extracter = new DoubleExtractor(columnIndex);
      break;
    case BINARY:
      extracter = new BinaryExtractorByValue(columnIndex);
      break;
    case STRING:
      extracter = new StringExtractorByValue(columnIndex);
      break;
    case VARCHAR:
      extracter = new VarCharExtractorByValue((VarcharTypeInfo) primitiveTypeInfo, columnIndex);
      break;
    case CHAR:
      extracter = new CharExtractorByValue((CharTypeInfo) primitiveTypeInfo, columnIndex);
      break;
    case DECIMAL:
      extracter = new DecimalExtractor((DecimalTypeInfo) primitiveTypeInfo, columnIndex);
      break;
    case INTERVAL_YEAR_MONTH:
      extracter = new IntervalYearMonthExtractor(columnIndex);
      break;
    case INTERVAL_DAY_TIME:
      extracter = new IntervalDayTimeExtractor(columnIndex);
      break;
    default:
      throw new HiveException("No vector row extracter for primitive category " +
          primitiveCategory);
    }
    return extracter;
  }

  Extractor[] extracters;

  public void init(StructObjectInspector structObjectInspector, List<Integer> projectedColumns) throws HiveException {

    extracters = new Extractor[projectedColumns.size()];

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();

    int i = 0;
    for (StructField field : fields) {
      int columnIndex = projectedColumns.get(i);
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(
          fieldInspector.getTypeName());
      extracters[i] = createExtractor(primitiveTypeInfo, columnIndex);
      i++;
    }
  }

  public void init(List<String> typeNames) throws HiveException {

    extracters = new Extractor[typeNames.size()];

    int i = 0;
    for (String typeName : typeNames) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      extracters[i] = createExtractor(primitiveTypeInfo, i);
      i++;
    }
  }

  public int getCount() {
    return extracters.length;
  }

  protected void setBatch(VectorizedRowBatch batch) throws HiveException {

    for (int i = 0; i < extracters.length; i++) {
      Extractor extracter = extracters[i];
      int columnIndex = extracter.getColumnIndex();
      if (batch.cols[columnIndex] == null) {
        if (tolerateNullColumns) {
          // Replace with void...
          extracter = new VoidExtractor(columnIndex);
          extracters[i] = extracter;
        } else {
          throw new HiveException("Unexpected null vector column " + columnIndex);
        }
      }
      extracter.setColumnVector(batch);
    }
  }

  protected void forgetBatch() {
    for (Extractor extracter : extracters) {
      extracter.forgetColumnVector();
    }
  }

  public Object extractRowColumn(int batchIndex, int logicalColumnIndex) {
    return extracters[logicalColumnIndex].extract(batchIndex);
  }

  public void extractRow(int batchIndex, Object[] objects) {
    int i = 0;
    for (Extractor extracter : extracters) {
      objects[i++] = extracter.extract(batchIndex);
    }
  }
}