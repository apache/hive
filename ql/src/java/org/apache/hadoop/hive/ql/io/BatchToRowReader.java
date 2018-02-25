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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io;


import com.google.common.collect.Lists;

import org.apache.hadoop.hive.llap.DebugUtils;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A record reader wrapper that converts VRB reader into an OI-based reader.
 * Due to the fact that changing table OIs in the plan after compilation is nearly impossible,
 * this is made an abstract class where type-specific implementations can plug in certain details,
 * so that the data produced after wrapping a vectorized reader would conform to the original OIs.
 */
public abstract class BatchToRowReader<StructType, UnionType>
    implements RecordReader<NullWritable, Object> {
  protected static final Logger LOG = LoggerFactory.getLogger(BatchToRowReader.class);

  private final NullWritable key;
  private final VectorizedRowBatch batch;
  private final RecordReader<NullWritable, VectorizedRowBatch> vrbReader;

  private final List<TypeInfo> schema;
  private final boolean[] included;
  private int rowInBatch = 0;

  public BatchToRowReader(RecordReader<NullWritable, VectorizedRowBatch> vrbReader,
      VectorizedRowBatchCtx vrbCtx, List<Integer> includedCols) {
    this.vrbReader = vrbReader;
    this.key = vrbReader.createKey();
    this.batch = vrbReader.createValue();
    this.schema = Lists.<TypeInfo>newArrayList(vrbCtx.getRowColumnTypeInfos());
    // TODO: does this include partition columns?
    boolean[] included = new boolean[schema.size()];
    if (includedCols != null) {
      for (int colIx : includedCols) {
        included[colIx] = true;
      }
    } else {
      Arrays.fill(included, true);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Including the columns " + DebugUtils.toString(included));
    }
    this.included = included;
  }

  protected abstract StructType createStructObject(Object previous, List<TypeInfo> childrenTypes);
  protected abstract void setStructCol(StructType structObj, int i, Object value);
  protected abstract Object getStructCol(StructType structObj, int i);
  protected abstract UnionType createUnionObject(List<TypeInfo> childrenTypes, Object previous);
  protected abstract void setUnion(UnionType unionObj, byte tag, Object object);
  protected abstract Object getUnionField(UnionType unionObj);

  @Override
  public NullWritable createKey() {
    return key;
  }

  @Override
  public Object createValue() {
    return createStructObject(null, schema);
  }

  @Override
  public long getPos() throws IOException {
    return -1;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  public boolean next(NullWritable key, Object previous) throws IOException {
    if (!ensureBatch()) {
      return false;
    }
    @SuppressWarnings("unchecked")
    StructType value = (StructType)previous;
    for (int i = 0; i < schema.size(); ++i) {
      if (!included[i]) continue; // TODO: shortcut for last col below length?
      try {
        setStructCol(value, i,
            nextValue(batch.cols[i], rowInBatch, schema.get(i), getStructCol(value, i)));
      } catch (Throwable t) {
        LOG.error("Error at row " + rowInBatch + "/" + batch.size + ", column " + i
            + "/" + schema.size() + " " + batch.cols[i], t);
        throw (t instanceof IOException) ? (IOException)t : new IOException(t);
      }
    }
    ++rowInBatch;
    return true;
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   */
  private boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return vrbReader.next(key, batch) && batch.size > 0;
    }
    return true;
  }


  @Override
  public void close() throws IOException {
    vrbReader.close();
    batch.cols = null;
  }

  /* Routines for stubbing into Writables */

  public static BooleanWritable nextBoolean(ColumnVector vector,
                                     int row,
                                     Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      BooleanWritable result;
      if (previous == null || previous.getClass() != BooleanWritable.class) {
        result = new BooleanWritable();
      } else {
        result = (BooleanWritable) previous;
      }
      result.set(((LongColumnVector) vector).vector[row] != 0);
      return result;
    } else {
      return null;
    }
  }

  public static ByteWritable nextByte(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      ByteWritable result;
      if (previous == null || previous.getClass() != ByteWritable.class) {
        result = new ByteWritable();
      } else {
        result = (ByteWritable) previous;
      }
      result.set((byte) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static ShortWritable nextShort(ColumnVector vector,
                                 int row,
                                 Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      ShortWritable result;
      if (previous == null || previous.getClass() != ShortWritable.class) {
        result = new ShortWritable();
      } else {
        result = (ShortWritable) previous;
      }
      result.set((short) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static IntWritable nextInt(ColumnVector vector,
                             int row,
                             Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      IntWritable result;
      if (previous == null || previous.getClass() != IntWritable.class) {
        result = new IntWritable();
      } else {
        result = (IntWritable) previous;
      }
      result.set((int) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static LongWritable nextLong(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      LongWritable result;
      if (previous == null || previous.getClass() != LongWritable.class) {
        result = new LongWritable();
      } else {
        result = (LongWritable) previous;
      }
      result.set(((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static FloatWritable nextFloat(ColumnVector vector,
                                 int row,
                                 Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      FloatWritable result;
      if (previous == null || previous.getClass() != FloatWritable.class) {
        result = new FloatWritable();
      } else {
        result = (FloatWritable) previous;
      }
      result.set((float) ((DoubleColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static DoubleWritable nextDouble(ColumnVector vector,
                                   int row,
                                   Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      DoubleWritable result;
      if (previous == null || previous.getClass() != DoubleWritable.class) {
        result = new DoubleWritable();
      } else {
        result = (DoubleWritable) previous;
      }
      result.set(((DoubleColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static Text nextString(ColumnVector vector,
                         int row,
                         Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      Text result;
      if (previous == null || previous.getClass() != Text.class) {
        result = new Text();
      } else {
        result = (Text) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.vector[row], bytes.start[row], bytes.length[row]);
      return result;
    } else {
      return null;
    }
  }

  public static HiveCharWritable nextChar(ColumnVector vector,
                                   int row,
                                   int size,
                                   Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      HiveCharWritable result;
      if (previous == null || previous.getClass() != HiveCharWritable.class) {
        result = new HiveCharWritable();
      } else {
        result = (HiveCharWritable) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.toString(row), size);
      return result;
    } else {
      return null;
    }
  }

  public static HiveVarcharWritable nextVarchar(
      ColumnVector vector, int row, int size, Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      HiveVarcharWritable result;
      if (previous == null || previous.getClass() != HiveVarcharWritable.class) {
        result = new HiveVarcharWritable();
      } else {
        result = (HiveVarcharWritable) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.toString(row), size);
      return result;
    } else {
      return null;
    }
  }

  public static BytesWritable nextBinary(ColumnVector vector,
                                  int row,
                                  Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      BytesWritable result;
      if (previous == null || previous.getClass() != BytesWritable.class) {
        result = new BytesWritable();
      } else {
        result = (BytesWritable) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.vector[row], bytes.start[row], bytes.length[row]);
      return result;
    } else {
      return null;
    }
  }

  public static HiveDecimalWritable nextDecimal(ColumnVector vector,
                                         int row,
                                         Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      HiveDecimalWritable result;
      if (previous == null || previous.getClass() != HiveDecimalWritable.class) {
        result = new HiveDecimalWritable();
      } else {
        result = (HiveDecimalWritable) previous;
      }
      result.set(((DecimalColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  public static DateWritable nextDate(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      DateWritable result;
      if (previous == null || previous.getClass() != DateWritable.class) {
        result = new DateWritable();
      } else {
        result = (DateWritable) previous;
      }
      int date = (int) ((LongColumnVector) vector).vector[row];
      result.set(date);
      return result;
    } else {
      return null;
    }
  }

  public static TimestampWritable nextTimestamp(ColumnVector vector,
                                         int row,
                                         Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      TimestampWritable result;
      if (previous == null || previous.getClass() != TimestampWritable.class) {
        result = new TimestampWritable();
      } else {
        result = (TimestampWritable) previous;
      }
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      result.setInternal(tcv.time[row], tcv.nanos[row]);
      return result;
    } else {
      return null;
    }
  }

  public StructType nextStruct(
      ColumnVector vector, int row, StructTypeInfo schema, Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      List<TypeInfo> childrenTypes = schema.getAllStructFieldTypeInfos();
      StructType result = createStructObject(previous, childrenTypes);
      StructColumnVector struct = (StructColumnVector) vector;
      for (int f = 0; f < childrenTypes.size(); ++f) {
        setStructCol(result, f, nextValue(struct.fields[f], row,
            childrenTypes.get(f), getStructCol(result, f)));
      }
      return result;
    } else {
      return null;
    }
  }

  private UnionType nextUnion(
      ColumnVector vector, int row, UnionTypeInfo schema, Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      List<TypeInfo> childrenTypes = schema.getAllUnionObjectTypeInfos();
      UnionType result = createUnionObject(childrenTypes, previous);
      UnionColumnVector union = (UnionColumnVector) vector;
      byte tag = (byte) union.tags[row];
      setUnion(result, tag, nextValue(union.fields[tag], row, childrenTypes.get(tag),
          getUnionField(result)));
      return result;
    } else {
      return null;
    }
  }

  private ArrayList<Object> nextList(
      ColumnVector vector, int row, ListTypeInfo schema, Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      ArrayList<Object> result;
      if (previous == null || previous.getClass() != ArrayList.class) {
        result = new ArrayList<>();
      } else {
        result = (ArrayList<Object>) previous;
      }
      ListColumnVector list = (ListColumnVector) vector;
      int length = (int) list.lengths[row];
      int offset = (int) list.offsets[row];
      result.ensureCapacity(length);
      int oldLength = result.size();
      int idx = 0;
      TypeInfo childType = schema.getListElementTypeInfo();
      while (idx < length && idx < oldLength) {
        result.set(idx, nextValue(list.child, offset + idx, childType,
            result.get(idx)));
        idx += 1;
      }
      if (length < oldLength) {
        for(int i= oldLength - 1; i >= length; --i) {
          result.remove(i);
        }
      } else if (oldLength < length) {
        while (idx < length) {
          result.add(nextValue(list.child, offset + idx, childType, null));
          idx += 1;
        }
      }
      return result;
    } else {
      return null;
    }
  }

  private HashMap<Object,Object> nextMap(
      ColumnVector vector, int row, MapTypeInfo schema, Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      MapColumnVector map = (MapColumnVector) vector;
      int length = (int) map.lengths[row];
      int offset = (int) map.offsets[row];
      TypeInfo keyType = schema.getMapKeyTypeInfo();
      TypeInfo valueType = schema.getMapValueTypeInfo();
      HashMap<Object,Object> result;
      if (previous == null || previous.getClass() != HashMap.class) {
        result = new HashMap<Object,Object>(length);
      } else {
        result = (HashMap<Object,Object>) previous;
        // I couldn't think of a good way to reuse the keys and value objects
        // without even more allocations, so take the easy and safe approach.
        result.clear();
      }
      for(int e=0; e < length; ++e) {
        result.put(nextValue(map.keys, e + offset, keyType, null),
                   nextValue(map.values, e + offset, valueType, null));
      }
      return result;
    } else {
      return null;
    }
  }

  private Object nextValue(ColumnVector vector, int row, TypeInfo schema, Object previous) {
    switch (schema.getCategory()) {
      case STRUCT:
        return nextStruct(vector, row, (StructTypeInfo)schema, previous);
      case UNION:
        return nextUnion(vector, row, (UnionTypeInfo)schema, previous);
      case LIST:
        return nextList(vector, row, (ListTypeInfo)schema, previous);
      case MAP:
        return nextMap(vector, row, (MapTypeInfo)schema, previous);
      case PRIMITIVE: {
        PrimitiveTypeInfo pschema = (PrimitiveTypeInfo)schema;
        switch (pschema.getPrimitiveCategory()) {
        case BOOLEAN:
          return nextBoolean(vector, row, previous);
        case BYTE:
          return nextByte(vector, row, previous);
        case SHORT:
          return nextShort(vector, row, previous);
        case INT:
          return nextInt(vector, row, previous);
        case LONG:
          return nextLong(vector, row, previous);
        case FLOAT:
          return nextFloat(vector, row, previous);
        case DOUBLE:
          return nextDouble(vector, row, previous);
        case STRING:
          return nextString(vector, row, previous);
        case CHAR:
          return nextChar(vector, row, ((CharTypeInfo)pschema).getLength(), previous);
        case VARCHAR:
          return nextVarchar(vector, row, ((VarcharTypeInfo)pschema).getLength(), previous);
        case BINARY:
          return nextBinary(vector, row, previous);
        case DECIMAL:
          return nextDecimal(vector, row, previous);
        case DATE:
          return nextDate(vector, row, previous);
        case TIMESTAMP:
          return nextTimestamp(vector, row, previous);
        default:
          throw new IllegalArgumentException("Unknown type " + schema);
        }
      }
      default:
        throw new IllegalArgumentException("Unknown type " + schema);
    }
  }
}
