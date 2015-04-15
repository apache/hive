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

import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

/**
 * This class assigns specified columns of a row from a Writable row Object[].
 *
 * The caller provides the hive type names and target column numbers in the order desired to
 * assign from the Writable row Object[].
 *
 * This class is abstract to allow the subclasses to control batch reuse.
 */
public abstract class VectorAssignRow {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorAssignRow.class);

  protected abstract class Assigner {
    protected int columnIndex;

    Assigner(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    public int getColumnIndex() {
      return columnIndex;
    }

    abstract void setColumnVector(VectorizedRowBatch batch);

    abstract void forgetColumnVector();

    abstract void assign(int batchIndex, Object object);
  }

  private class VoidAssigner extends Assigner {

    VoidAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void setColumnVector(VectorizedRowBatch batch) {
    }

    @Override
    void forgetColumnVector() {
    }

    @Override
    void assign(int batchIndex, Object object) {
      // This is no-op, there is no column to assign to and the object is expected to be null.
      assert (object == null);
    }
  }

  private abstract class AbstractLongAssigner extends Assigner {

    protected LongColumnVector colVector;
    protected long[] vector;

    AbstractLongAssigner(int columnIndex) {
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

  protected class BooleanAssigner extends AbstractLongAssigner {

    BooleanAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        BooleanWritable bw = (BooleanWritable) object;
        vector[batchIndex] = (bw.get() ? 1 : 0);
      }
    }
  }

  protected class ByteAssigner extends AbstractLongAssigner {

    ByteAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        ByteWritable bw = (ByteWritable) object;
        vector[batchIndex] = bw.get();
      }
    }
  }

  private class ShortAssigner extends AbstractLongAssigner {

    ShortAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        ShortWritable sw = (ShortWritable) object;
        vector[batchIndex] = sw.get();
      }
    }
  }

  private class IntAssigner extends AbstractLongAssigner {

    IntAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        IntWritable iw = (IntWritable) object;
        vector[batchIndex] = iw.get();
      }
    }
  }

  private class LongAssigner extends AbstractLongAssigner {

    LongAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        LongWritable lw = (LongWritable) object;
        vector[batchIndex] = lw.get();
      }
    }
  }

  private class DateAssigner extends AbstractLongAssigner {

    DateAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        DateWritable bw = (DateWritable) object;
        vector[batchIndex] = bw.getDays();
      }
    }
  }

  private class TimestampAssigner extends AbstractLongAssigner {

    TimestampAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        TimestampWritable tw = (TimestampWritable) object;
        Timestamp t = tw.getTimestamp();
        vector[batchIndex] = TimestampUtils.getTimeNanoSec(t);
      }
    }
  }

  private class IntervalYearMonthAssigner extends AbstractLongAssigner {

    IntervalYearMonthAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        HiveIntervalYearMonthWritable iymw = (HiveIntervalYearMonthWritable) object;
        HiveIntervalYearMonth iym = iymw.getHiveIntervalYearMonth();
        vector[batchIndex] = iym.getTotalMonths();
      }
    }
  }

  private class IntervalDayTimeAssigner extends AbstractLongAssigner {

    IntervalDayTimeAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        HiveIntervalDayTimeWritable idtw = (HiveIntervalDayTimeWritable) object;
        HiveIntervalDayTime idt = idtw.getHiveIntervalDayTime();
        vector[batchIndex] = DateUtils.getIntervalDayTimeTotalNanos(idt);
      }
    }
  }

  private abstract class AbstractDoubleAssigner extends Assigner {

    protected DoubleColumnVector colVector;
    protected double[] vector;

    AbstractDoubleAssigner(int columnIndex) {
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

  private class FloatAssigner extends AbstractDoubleAssigner {

    FloatAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        FloatWritable fw = (FloatWritable) object;
        vector[batchIndex] = fw.get();
      }
    }
  }

  private class DoubleAssigner extends AbstractDoubleAssigner {

    DoubleAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        DoubleWritable dw = (DoubleWritable) object;
        vector[batchIndex] = dw.get();
      }
    }
  }

  private abstract class AbstractBytesAssigner extends Assigner {

    protected BytesColumnVector colVector;

    AbstractBytesAssigner(int columnIndex) {
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

  private class BinaryAssigner extends AbstractBytesAssigner {

    BinaryAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        BytesWritable bw = (BytesWritable) object;
        colVector.setVal(batchIndex, bw.getBytes(), 0, bw.getLength());
      }
    }
  }

  private class StringAssigner extends AbstractBytesAssigner {

    StringAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        Text tw = (Text) object;
        colVector.setVal(batchIndex, tw.getBytes(), 0, tw.getLength());
      }
    }
  }

  private class VarCharAssigner extends AbstractBytesAssigner {

    VarCharAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // We store VARCHAR type stripped of pads.
        HiveVarchar hiveVarchar;
        if (object instanceof HiveVarchar) {
          hiveVarchar = (HiveVarchar) object;
        } else {
          hiveVarchar = ((HiveVarcharWritable) object).getHiveVarchar();
        }
        byte[] bytes = hiveVarchar.getValue().getBytes();
        colVector.setVal(batchIndex, bytes, 0, bytes.length);
      }
    }
  }

  private class CharAssigner extends AbstractBytesAssigner {

    CharAssigner(int columnIndex) {
      super(columnIndex);
    }

    @Override
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        // We store CHAR type stripped of pads.
        HiveChar hiveChar;
        if (object instanceof HiveChar) {
          hiveChar = (HiveChar) object;
        } else {
          hiveChar = ((HiveCharWritable) object).getHiveChar();
        }

        // We store CHAR in vector row batch with padding stripped.
        byte[] bytes = hiveChar.getStrippedValue().getBytes();
        colVector.setVal(batchIndex, bytes, 0, bytes.length);
      }
    }
  }

  private class DecimalAssigner extends Assigner {

    protected DecimalColumnVector colVector;

    DecimalAssigner(int columnIndex) {
      super(columnIndex);
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
    void assign(int batchIndex, Object object) {
      if (object == null) {
        VectorizedBatchUtil.setNullColIsNullValue(colVector, batchIndex);
      } else {
        if (object instanceof HiveDecimal) {
          colVector.set(batchIndex, (HiveDecimal) object); 
        } else {
          colVector.set(batchIndex, (HiveDecimalWritable) object);
        }
      }
    }
  }

  private Assigner createAssigner(PrimitiveTypeInfo primitiveTypeInfo, int columnIndex) throws HiveException {
    PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
    Assigner assigner;
    switch (primitiveCategory) {
    case VOID:
      assigner = new VoidAssigner(columnIndex);
      break;
    case BOOLEAN:
      assigner = new BooleanAssigner(columnIndex);
      break;
    case BYTE:
      assigner = new ByteAssigner(columnIndex);
      break;
    case SHORT:
      assigner = new ShortAssigner(columnIndex);
      break;
    case INT:
      assigner = new IntAssigner(columnIndex);
      break;
    case LONG:
      assigner = new LongAssigner(columnIndex);
      break;
    case TIMESTAMP:
      assigner = new TimestampAssigner(columnIndex);
      break;
    case DATE:
      assigner = new DateAssigner(columnIndex);
      break;
    case FLOAT:
      assigner = new FloatAssigner(columnIndex);
      break;
    case DOUBLE:
      assigner = new DoubleAssigner(columnIndex);
      break;
    case BINARY:
      assigner = new BinaryAssigner(columnIndex);
      break;
    case STRING:
      assigner = new StringAssigner(columnIndex);
      break;
    case VARCHAR:
      assigner = new VarCharAssigner(columnIndex);
      break;
    case CHAR:
      assigner = new CharAssigner(columnIndex);
      break;
    case DECIMAL:
      assigner = new DecimalAssigner(columnIndex);
      break;
    case INTERVAL_YEAR_MONTH:
      assigner = new IntervalYearMonthAssigner(columnIndex);
      break;
    case INTERVAL_DAY_TIME:
      assigner = new IntervalDayTimeAssigner(columnIndex);
      break;
    default:
      throw new HiveException("No vector row assigner for primitive category " +
          primitiveCategory);
    }
    return assigner;
  }

  Assigner[] assigners;

  public void init(StructObjectInspector structObjectInspector, List<Integer> projectedColumns) throws HiveException {

    List<? extends StructField> fields = structObjectInspector.getAllStructFieldRefs();
    assigners = new Assigner[fields.size()];

    int i = 0;
    for (StructField field : fields) {
      int columnIndex = projectedColumns.get(i);
      ObjectInspector fieldInspector = field.getFieldObjectInspector();
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(
                  fieldInspector.getTypeName());
      assigners[i] = createAssigner(primitiveTypeInfo, columnIndex);
      i++;
    }
  }

  public void init(List<String> typeNames) throws HiveException {

    assigners = new Assigner[typeNames.size()];

    int i = 0;
    for (String typeName : typeNames) {
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      assigners[i] = createAssigner(primitiveTypeInfo, i);
      i++;
    }
  }

  protected void setBatch(VectorizedRowBatch batch) throws HiveException {
    for (int i = 0; i < assigners.length; i++) {
      Assigner assigner = assigners[i];
      int columnIndex = assigner.getColumnIndex();
      if (batch.cols[columnIndex] == null) {
        throw new HiveException("Unexpected null vector column " + columnIndex);
      }
      assigner.setColumnVector(batch);
    }
  }

  protected void forgetBatch() {
    for (Assigner assigner : assigners) {
      assigner.forgetColumnVector();
    }
  }

  public void assignRowColumn(int batchIndex, int logicalColumnIndex, Object object) {
    assigners[logicalColumnIndex].assign(batchIndex, object);
  }

  public void assignRow(int batchIndex, Object[] objects) {
    int i = 0;
    for (Assigner assigner : assigners) {
      assigner.assign(batchIndex, objects[i++]);
    }
  }

}