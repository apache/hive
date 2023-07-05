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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordReaderImpl extends org.apache.orc.impl.RecordReaderImpl
                              implements RecordReader {
  static final Logger LOG = LoggerFactory.getLogger(RecordReaderImpl.class);
  private final VectorizedRowBatch batch;
  private int rowInBatch;
  private long baseRow;

  protected RecordReaderImpl(ReaderImpl fileReader,
    Reader.Options options, final Configuration conf) throws IOException {
    super(fileReader, options);
    final boolean useDecimal64ColumnVectors = conf != null && HiveConf.getVar(conf,
      HiveConf.ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
    if (useDecimal64ColumnVectors){
      batch = this.schema.createRowBatchV2();
    } else {
      batch = this.schema.createRowBatch();
    }
    rowInBatch = 0;
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      baseRow = super.getRowNumber();
      rowInBatch = 0;
      return super.nextBatch(batch);
    }
    return true;
  }

  public VectorizedRowBatch createRowBatch(boolean useDecimal64) {
    return useDecimal64 ? this.schema.createRowBatchV2() : this.schema.createRowBatch();
  }

  @Override
  public long getRowNumber() {
    return baseRow + rowInBatch;
  }

  @Override
  public boolean hasNext() throws IOException {
    return ensureBatch();
  }

  @Override
  public void seekToRow(long row) throws IOException {
    if (row >= baseRow && row < baseRow + batch.size) {
      rowInBatch = (int) (row - baseRow);
    } else {
      super.seekToRow(row);
      batch.size = 0;
      ensureBatch();
    }
  }

  @Override
  public Object next(Object previous) throws IOException {
    if (!ensureBatch()) {
      return null;
    }
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      OrcStruct result;
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      if (previous == null || previous.getClass() != OrcStruct.class) {
        result = new OrcStruct(numberOfChildren);
        previous = result;
      } else {
        result = (OrcStruct) previous;
        if (result.getNumFields() != numberOfChildren) {
          result.setNumFields(numberOfChildren);
        }
      }
      for(int i=0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, nextValue(batch.cols[i], rowInBatch,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      previous = nextValue(batch.cols[0], rowInBatch, schema, previous);
    }
    rowInBatch += 1;
    return previous;
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch theirBatch) throws IOException {
    // If the user hasn't been reading by row, use the fast path.
    if (rowInBatch >= batch.size) {
      if (batch.size > 0) {
        // the local batch has been consumed entirely, reset it
        batch.reset();
      }
      baseRow = super.getRowNumber();
      rowInBatch = 0;
      return super.nextBatch(theirBatch);
    }
    copyIntoBatch(theirBatch, batch, rowInBatch);
    rowInBatch += theirBatch.size;
    return theirBatch.size > 0;
  }

  @Override
  public void close() throws IOException {
    super.close();
    // free the memory for the column vectors
    if (batch != null) {
      batch.cols = null;
    }
  }

  /* Routines for stubbing into Writables */

  static BooleanWritable nextBoolean(ColumnVector vector,
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

  static ByteWritable nextByte(ColumnVector vector,
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

  static ShortWritable nextShort(ColumnVector vector,
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

  static IntWritable nextInt(ColumnVector vector,
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

  static LongWritable nextLong(ColumnVector vector,
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

  static FloatWritable nextFloat(ColumnVector vector,
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

  static DoubleWritable nextDouble(ColumnVector vector,
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

  static Text nextString(ColumnVector vector,
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

  static HiveCharWritable nextChar(ColumnVector vector,
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

  static HiveVarcharWritable nextVarchar(ColumnVector vector,
                                         int row,
                                         int size,
                                         Object previous) {
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

  static BytesWritable nextBinary(ColumnVector vector,
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

  static HiveDecimalWritable nextDecimal(ColumnVector vector,
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
      if (vector instanceof Decimal64ColumnVector) {
        long value = ((Decimal64ColumnVector) vector).vector[row];
        result.deserialize64(value, ((Decimal64ColumnVector) vector).scale);
      } else {
        result.set(((DecimalColumnVector) vector).vector[row]);
      }
      return result;
    } else {
      return null;
    }
  }

  static DateWritableV2 nextDate(ColumnVector vector,
                                 int row,
                                 Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      DateWritableV2 result;
      if (previous == null || previous.getClass() != DateWritableV2.class) {
        result = new DateWritableV2();
      } else {
        result = (DateWritableV2) previous;
      }
      int date = (int) ((DateColumnVector) vector).vector[row];
      result.set(date);
      return result;
    } else {
      return null;
    }
  }

  static TimestampWritableV2 nextTimestamp(ColumnVector vector,
                                           int row,
                                           Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      TimestampWritableV2 result;
      if (previous == null || previous.getClass() != TimestampWritableV2.class) {
        result = new TimestampWritableV2();
      } else {
        result = (TimestampWritableV2) previous;
      }
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      result.setInternal(tcv.time[row], tcv.nanos[row]);
      return result;
    } else {
      return null;
    }
  }

  static OrcStruct nextStruct(ColumnVector vector,
                              int row,
                              TypeDescription schema,
                              Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcStruct result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      int numChildren = childrenTypes.size();
      if (previous == null || previous.getClass() != OrcStruct.class) {
        result = new OrcStruct(numChildren);
      } else {
        result = (OrcStruct) previous;
        result.setNumFields(numChildren);
      }
      StructColumnVector struct = (StructColumnVector) vector;
      for(int f=0; f < numChildren; ++f) {
        result.setFieldValue(f, nextValue(struct.fields[f], row,
            childrenTypes.get(f), result.getFieldValue(f)));
      }
      return result;
    } else {
      return null;
    }
  }

  static OrcUnion nextUnion(ColumnVector vector,
                            int row,
                            TypeDescription schema,
                            Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcUnion result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      if (previous == null || previous.getClass() != OrcUnion.class) {
        result = new OrcUnion();
      } else {
        result = (OrcUnion) previous;
      }
      UnionColumnVector union = (UnionColumnVector) vector;
      byte tag = (byte) union.tags[row];
      result.set(tag, nextValue(union.fields[tag], row, childrenTypes.get(tag),
          result.getObject()));
      return result;
    } else {
      return null;
    }
  }

  static ArrayList<Object> nextList(ColumnVector vector,
                                    int row,
                                    TypeDescription schema,
                                    Object previous) {
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
      TypeDescription childType = schema.getChildren().get(0);
      while (idx < length && idx < oldLength) {
        result.set(idx, nextValue(list.child, offset + idx, childType,
            result.get(idx)));
        idx += 1;
      }
      if (length < oldLength) {
        result.subList(length,result.size()).clear();
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

  static Map<Object,Object> nextMap(ColumnVector vector,
                                    int row,
                                    TypeDescription schema,
                                    Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      MapColumnVector map = (MapColumnVector) vector;
      int length = (int) map.lengths[row];
      int offset = (int) map.offsets[row];
      TypeDescription keyType = schema.getChildren().get(0);
      TypeDescription valueType = schema.getChildren().get(1);
      LinkedHashMap<Object,Object> result;
      if (previous == null || previous.getClass() != LinkedHashMap.class) {
        result = new LinkedHashMap<Object,Object>(length);
      } else {
        result = (LinkedHashMap<Object,Object>) previous;
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

  static Object nextValue(ColumnVector vector,
                          int row,
                          TypeDescription schema,
                          Object previous) {
    switch (schema.getCategory()) {
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
        return nextChar(vector, row, schema.getMaxLength(), previous);
      case VARCHAR:
        return nextVarchar(vector, row, schema.getMaxLength(), previous);
      case BINARY:
        return nextBinary(vector, row, previous);
      case DECIMAL:
        return nextDecimal(vector, row, previous);
      case DATE:
        return nextDate(vector, row, previous);
      case TIMESTAMP:
        return nextTimestamp(vector, row, previous);
      case STRUCT:
        return nextStruct(vector, row, schema, previous);
      case UNION:
        return nextUnion(vector, row, schema, previous);
      case LIST:
        return nextList(vector, row, schema, previous);
      case MAP:
        return nextMap(vector, row, schema, previous);
      default:
        throw new IllegalArgumentException("Unknown type " + schema);
    }
  }

  /* Routines for copying between VectorizedRowBatches */

  void copyLongColumn(ColumnVector destination,
                      ColumnVector source,
                      int sourceOffset,
                      int length) {
    LongColumnVector lsource = (LongColumnVector) source;
    LongColumnVector ldest = (LongColumnVector) destination;
    ldest.isRepeating = lsource.isRepeating;
    ldest.noNulls = lsource.noNulls;
    if (source.isRepeating) {
      ldest.isNull[0] = lsource.isNull[0];
      ldest.vector[0] = lsource.vector[0];
    } else {
      if (!lsource.noNulls) {
        for(int r=0; r < length; ++r) {
          ldest.isNull[r] = lsource.isNull[sourceOffset + r];
          ldest.vector[r] = lsource.vector[sourceOffset + r];
        }
      } else {
        for (int r = 0; r < length; ++r) {
          ldest.vector[r] = lsource.vector[sourceOffset + r];
        }
      }
    }
  }

  void copyDoubleColumn(ColumnVector destination,
                        ColumnVector source,
                        int sourceOffset,
                        int length) {
    DoubleColumnVector castedSource = (DoubleColumnVector) source;
    DoubleColumnVector castedDestination = (DoubleColumnVector) destination;
    if (source.isRepeating) {
      castedDestination.isRepeating = true;
      castedDestination.noNulls = castedSource.noNulls;
      castedDestination.isNull[0] = castedSource.isNull[0];
      castedDestination.vector[0] = castedSource.vector[0];
    } else {
      if (!castedSource.noNulls) {
        castedDestination.noNulls = true;
        for(int r=0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
        }
      }
      for(int r=0; r < length; ++r) {
        castedDestination.vector[r] = castedSource.vector[sourceOffset + r];
      }
    }
  }

  void copyTimestampColumn(ColumnVector destination,
                           ColumnVector source,
                           int sourceOffset,
                           int length) {
    TimestampColumnVector castedSource = (TimestampColumnVector) source;
    TimestampColumnVector castedDestination = (TimestampColumnVector) destination;
    castedDestination.isRepeating = castedSource.isRepeating;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      castedDestination.time[0] = castedSource.time[0];
      castedDestination.nanos[0] = castedSource.nanos[0];
    } else {
      if (!castedSource.noNulls) {
        castedDestination.noNulls = true;
        for(int r=0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
          castedDestination.time[r] = castedSource.time[sourceOffset + r];
          castedDestination.nanos[r] = castedSource.nanos[sourceOffset + r];
        }
      } else {
        for (int r = 0; r < length; ++r) {
          castedDestination.time[r] = castedSource.time[sourceOffset + r];
          castedDestination.nanos[r] = castedSource.nanos[sourceOffset + r];
        }
      }
    }
  }

  void copyDecimalColumn(ColumnVector destination,
                         ColumnVector source,
                         int sourceOffset,
                         int length) {
    DecimalColumnVector castedSource = (DecimalColumnVector) source;
    DecimalColumnVector castedDestination = (DecimalColumnVector) destination;
    castedDestination.isRepeating = castedSource.isRepeating;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      if (!castedSource.isNull[0]) {
        castedDestination.set(0, castedSource.vector[0]);
      }
    } else {
      if (!castedSource.noNulls) {
        for(int r=0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
          if (!castedDestination.isNull[r]) {
            castedDestination.set(r, castedSource.vector[r]);
          }
        }
      } else {
        for (int r = 0; r < length; ++r) {
          castedDestination.set(r, castedSource.vector[r]);
        }
      }
    }
  }

  void copyBytesColumn(ColumnVector destination,
                       ColumnVector source,
                       int sourceOffset,
                       int length) {
    BytesColumnVector castedSource = (BytesColumnVector) source;
    BytesColumnVector castedDestination = (BytesColumnVector) destination;
    castedDestination.isRepeating = castedSource.isRepeating;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      if (!castedSource.isNull[0]) {
        castedDestination.setVal(0, castedSource.vector[0],
            castedSource.start[0], castedSource.length[0]);
      }
    } else {
      if (!castedSource.noNulls) {
        for(int r=0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
          if (!castedDestination.isNull[r]) {
            castedDestination.setVal(r, castedSource.vector[sourceOffset + r],
                castedSource.start[sourceOffset + r],
                castedSource.length[sourceOffset + r]);
          }
        }
      } else {
        for (int r = 0; r < length; ++r) {
          castedDestination.setVal(r, castedSource.vector[sourceOffset + r],
              castedSource.start[sourceOffset + r],
              castedSource.length[sourceOffset + r]);
        }
      }
    }
  }

  void copyStructColumn(ColumnVector destination,
                        ColumnVector source,
                        int sourceOffset,
                        int length) {
    StructColumnVector castedSource = (StructColumnVector) source;
    StructColumnVector castedDestination = (StructColumnVector) destination;
    castedDestination.isRepeating = castedSource.isRepeating;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      for(int c=0; c > castedSource.fields.length; ++c) {
        copyColumn(castedDestination.fields[c], castedSource.fields[c], 0, 1);
      }
    } else {
      if (!castedSource.noNulls) {
        for (int r = 0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
        }
      } else {
        for (int c = 0; c > castedSource.fields.length; ++c) {
          copyColumn(castedDestination.fields[c], castedSource.fields[c],
              sourceOffset, length);
        }
      }
    }
  }

  void copyUnionColumn(ColumnVector destination,
                       ColumnVector source,
                       int sourceOffset,
                       int length) {
    UnionColumnVector castedSource = (UnionColumnVector) source;
    UnionColumnVector castedDestination = (UnionColumnVector) destination;
    castedDestination.isRepeating = castedSource.isRepeating;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      int tag = castedSource.tags[0];
      castedDestination.tags[0] = tag;
      if (!castedDestination.isNull[0]) {
        copyColumn(castedDestination.fields[tag], castedSource.fields[tag], 0,
            1);
      }
    } else {
      if (!castedSource.noNulls) {
        for (int r = 0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
          castedDestination.tags[r] = castedSource.tags[sourceOffset + r];
        }
      } else {
        for(int r=0; r < length; ++r) {
          castedDestination.tags[r] = castedSource.tags[sourceOffset + r];
        }
      }
      for(int c=0; c > castedSource.fields.length; ++c) {
        copyColumn(castedDestination.fields[c], castedSource.fields[c],
            sourceOffset, length);
      }
    }
  }

  void copyListColumn(ColumnVector destination,
                       ColumnVector source,
                       int sourceOffset,
                       int length) {
    ListColumnVector castedSource = (ListColumnVector) source;
    ListColumnVector castedDestination = (ListColumnVector) destination;
    castedDestination.isRepeating = castedSource.noNulls;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      castedDestination.offsets[0] = 0;
      castedDestination.lengths[0] = castedSource.lengths[0];
      copyColumn(castedDestination.child, castedSource.child,
          (int) castedSource.offsets[0], (int) castedSource.lengths[0]);
    } else {
      if (!castedSource.noNulls) {
        for (int r = 0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
        }
      }
      int minOffset = Integer.MAX_VALUE;
      int maxOffset = Integer.MIN_VALUE;
      for(int r=0; r < length; ++r) {
        int childOffset = (int) castedSource.offsets[r + sourceOffset];
        int childLength = (int) castedSource.lengths[r + sourceOffset];
        castedDestination.offsets[r] = childOffset;
        castedDestination.lengths[r] = childLength;
        minOffset = Math.min(minOffset, childOffset);
        maxOffset = Math.max(maxOffset, childOffset + childLength);
      }
      if (minOffset <= maxOffset) {
        castedDestination.childCount = maxOffset - minOffset + 1;
        copyColumn(castedDestination.child, castedSource.child,
            minOffset, castedDestination.childCount);
      } else {
        castedDestination.childCount = 0;
      }
    }
  }

  void copyMapColumn(ColumnVector destination,
                     ColumnVector source,
                     int sourceOffset,
                     int length) {
    MapColumnVector castedSource = (MapColumnVector) source;
    MapColumnVector castedDestination = (MapColumnVector) destination;
    castedDestination.isRepeating = castedSource.noNulls;
    castedDestination.noNulls = castedSource.noNulls;
    if (source.isRepeating) {
      castedDestination.isNull[0] = castedSource.isNull[0];
      castedDestination.offsets[0] = 0;
      castedDestination.lengths[0] = castedSource.lengths[0];
      copyColumn(castedDestination.keys, castedSource.keys,
          (int) castedSource.offsets[0], (int) castedSource.lengths[0]);
      copyColumn(castedDestination.values, castedSource.values,
          (int) castedSource.offsets[0], (int) castedSource.lengths[0]);
    } else {
      if (!castedSource.noNulls) {
        for (int r = 0; r < length; ++r) {
          castedDestination.isNull[r] = castedSource.isNull[sourceOffset + r];
        }
      }
      int minOffset = Integer.MAX_VALUE;
      int maxOffset = Integer.MIN_VALUE;
      for(int r=0; r < length; ++r) {
        int childOffset = (int) castedSource.offsets[r + sourceOffset];
        int childLength = (int) castedSource.lengths[r + sourceOffset];
        castedDestination.offsets[r] = childOffset;
        castedDestination.lengths[r] = childLength;
        minOffset = Math.min(minOffset, childOffset);
        maxOffset = Math.max(maxOffset, childOffset + childLength);
      }
      if (minOffset <= maxOffset) {
        castedDestination.childCount = maxOffset - minOffset + 1;
        copyColumn(castedDestination.keys, castedSource.keys,
            minOffset, castedDestination.childCount);
        copyColumn(castedDestination.values, castedSource.values,
            minOffset, castedDestination.childCount);
      } else {
        castedDestination.childCount = 0;
      }
    }
  }

  void copyColumn(ColumnVector destination,
                  ColumnVector source,
                  int sourceOffset,
                  int length) {
    if (source.getClass() == LongColumnVector.class) {
      copyLongColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == DoubleColumnVector.class) {
      copyDoubleColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == BytesColumnVector.class) {
      copyBytesColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == TimestampColumnVector.class) {
      copyTimestampColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == DecimalColumnVector.class) {
      copyDecimalColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == StructColumnVector.class) {
      copyStructColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == UnionColumnVector.class) {
      copyUnionColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == ListColumnVector.class) {
      copyListColumn(destination, source, sourceOffset, length);
    } else if (source.getClass() == MapColumnVector.class) {
      copyMapColumn(destination, source, sourceOffset, length);
    }
  }

  /**
   * Copy part of a batch into the destination batch.
   * @param destination the batch to copy into
   * @param source the batch to copy from
   * @param sourceStart the row number to start from in the source
   * @return the number of rows copied
   */
  void copyIntoBatch(VectorizedRowBatch destination,
                     VectorizedRowBatch source,
                     int sourceStart) {
    int rows = Math.min(source.size - sourceStart, destination.getMaxSize());
    for(int c=0; c < source.cols.length; ++c) {
      destination.cols[c].reset();
      copyColumn(destination.cols[c], source.cols[c], sourceStart, rows);
    }
    destination.size = rows;
  }
}
