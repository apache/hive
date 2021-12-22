/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.List;

import static org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil.createColumnVector;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.MICROS_PER_SECOND;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.MILLIS_PER_SECOND;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.NS_PER_MICROS;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.NS_PER_MILLIS;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.NS_PER_SECOND;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.SECOND_PER_DAY;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.toStructListTypeInfo;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.toStructListVector;

class Deserializer {
  private final ArrowColumnarBatchSerDe serDe;
  private final VectorExtractRow vectorExtractRow;
  private final VectorizedRowBatch vectorizedRowBatch;
  private Object[][] rows;

  Deserializer(ArrowColumnarBatchSerDe serDe) throws SerDeException {
    this.serDe = serDe;
    vectorExtractRow = new VectorExtractRow();
    final List<TypeInfo> fieldTypeInfoList = serDe.rowTypeInfo.getAllStructFieldTypeInfos();
    final int fieldCount = fieldTypeInfoList.size();
    final TypeInfo[] typeInfos = fieldTypeInfoList.toArray(new TypeInfo[fieldCount]);
    try {
      vectorExtractRow.init(typeInfos);
    } catch (HiveException e) {
      throw new SerDeException(e);
    }

    vectorizedRowBatch = new VectorizedRowBatch(fieldCount);
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      final ColumnVector columnVector = createColumnVector(typeInfos[fieldIndex]);
      columnVector.init();
      vectorizedRowBatch.cols[fieldIndex] = columnVector;
    }
  }

  public Object deserialize(Writable writable) {
    final ArrowWrapperWritable arrowWrapperWritable = (ArrowWrapperWritable) writable;
    final VectorSchemaRoot vectorSchemaRoot = arrowWrapperWritable.getVectorSchemaRoot();
    final List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
    final int fieldCount = fieldVectors.size();
    final int rowCount = vectorSchemaRoot.getFieldVectors().get(0).getValueCount();
    vectorizedRowBatch.ensureSize(rowCount);

    if (rows == null || rows.length < rowCount ) {
      rows = new Object[rowCount][];
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        rows[rowIndex] = new Object[fieldCount];
      }
    }

    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
      final FieldVector fieldVector = fieldVectors.get(fieldIndex);
      final int projectedCol = vectorizedRowBatch.projectedColumns[fieldIndex];
      final ColumnVector columnVector = vectorizedRowBatch.cols[projectedCol];
      final TypeInfo typeInfo = serDe.rowTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
      read(fieldVector, columnVector, typeInfo);
    }
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      vectorExtractRow.extractRow(vectorizedRowBatch, rowIndex, rows[rowIndex]);
    }
    vectorizedRowBatch.reset();
    return rows;
  }

  private void read(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo) {
    // make sure that hiveVector is as big as arrowVector
    final int size = arrowVector.getValueCount();
    hiveVector.ensureSize(size, false);

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        readPrimitive(arrowVector, hiveVector);
        break;
      case LIST:
        readList(arrowVector, (ListColumnVector) hiveVector, (ListTypeInfo) typeInfo);
        break;
      case MAP:
        readMap(arrowVector, (MapColumnVector) hiveVector, (MapTypeInfo) typeInfo);
        break;
      case STRUCT:
        readStruct(arrowVector, (StructColumnVector) hiveVector, (StructTypeInfo) typeInfo);
        break;
      case UNION:
        readUnion(arrowVector, (UnionColumnVector) hiveVector, (UnionTypeInfo) typeInfo);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  private void readPrimitive(FieldVector arrowVector, ColumnVector hiveVector) {
    final Types.MinorType minorType = arrowVector.getMinorType();

    final int size = arrowVector.getValueCount();

    switch (minorType) {
      case BIT:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((BitVector) arrowVector).get(i);
            }
          }
        }
        break;
      case TINYINT:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((TinyIntVector) arrowVector).get(i);
            }
          }
        }
        break;
      case SMALLINT:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((SmallIntVector) arrowVector).get(i);
            }
          }
        }
        break;
      case INT:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((IntVector) arrowVector).get(i);
            }
          }
        }
        break;
      case BIGINT:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((BigIntVector) arrowVector).get(i);
            }
          }
        }
        break;
      case FLOAT4:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((DoubleColumnVector) hiveVector).vector[i] = ((Float4Vector) arrowVector).get(i);
            }
          }
        }
        break;
      case FLOAT8:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((DoubleColumnVector) hiveVector).vector[i] = ((Float8Vector) arrowVector).get(i);
            }
          }
        }
        break;
      case VARCHAR:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((BytesColumnVector) hiveVector).setVal(i, ((VarCharVector) arrowVector).get(i));
            }
          }
        }
        break;
      case DATEDAY:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((DateDayVector) arrowVector).get(i);
            }
          }
        }
        break;
      case TIMESTAMPMILLI:
      case TIMESTAMPMILLITZ:
      case TIMESTAMPMICRO:
      case TIMESTAMPMICROTZ:
      case TIMESTAMPNANO:
      case TIMESTAMPNANOTZ:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;

              // Time = second + sub-second
              final long time = ((TimeStampVector) arrowVector).get(i);
              long second;
              int subSecondInNanos;
              switch (minorType) {
                case TIMESTAMPMILLI:
                case TIMESTAMPMILLITZ:
                  {
                    subSecondInNanos = (int) ((time % MILLIS_PER_SECOND) * NS_PER_MILLIS);
                    second = time / MILLIS_PER_SECOND;
                  }
                  break;
                case TIMESTAMPMICROTZ:
                case TIMESTAMPMICRO:
                  {
                    subSecondInNanos = (int) ((time % MICROS_PER_SECOND) * NS_PER_MICROS);
                    second = time / MICROS_PER_SECOND;
                  }
                  break;
                case TIMESTAMPNANOTZ:
                case TIMESTAMPNANO:
                  {
                    subSecondInNanos = (int) (time % NS_PER_SECOND);
                    second = time / NS_PER_SECOND;
                  }
                  break;
                default:
                  throw new IllegalArgumentException();
              }

              final TimestampColumnVector timestampColumnVector = (TimestampColumnVector) hiveVector;
              // A nanosecond value should not be negative
              if (subSecondInNanos < 0) {

                // So add one second to the negative nanosecond value to make it positive
                subSecondInNanos += NS_PER_SECOND;

                // Subtract one second from the second value because we added one second
                second -= 1;
              }
              timestampColumnVector.time[i] = second * MILLIS_PER_SECOND;
              timestampColumnVector.nanos[i] = subSecondInNanos;
            }
          }
        }
        break;
      case VARBINARY:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((BytesColumnVector) hiveVector).setVal(i, ((VarBinaryVector) arrowVector).get(i));
            }
          }
        }
        break;
      case DECIMAL:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((DecimalColumnVector) hiveVector).set(i,
                  HiveDecimal.create(((DecimalVector) arrowVector).getObject(i)));
            }
          }
        }
        break;
      case INTERVALYEAR:
        {
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              ((LongColumnVector) hiveVector).vector[i] = ((IntervalYearVector) arrowVector).get(i);
            }
          }
        }
        break;
      case INTERVALDAY:
        {
          final IntervalDayVector intervalDayVector = (IntervalDayVector) arrowVector;
          final NullableIntervalDayHolder intervalDayHolder = new NullableIntervalDayHolder();
          final HiveIntervalDayTime intervalDayTime = new HiveIntervalDayTime();
          for (int i = 0; i < size; i++) {
            if (arrowVector.isNull(i)) {
              VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
            } else {
              hiveVector.isNull[i] = false;
              intervalDayVector.get(i, intervalDayHolder);
              final long seconds = intervalDayHolder.days * SECOND_PER_DAY +
                  intervalDayHolder.milliseconds / MILLIS_PER_SECOND;
              final int nanos = (intervalDayHolder.milliseconds % 1_000) * NS_PER_MILLIS;
              intervalDayTime.set(seconds, nanos);
              ((IntervalDayTimeColumnVector) hiveVector).set(i, intervalDayTime);
            }
          }
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  private void readList(FieldVector arrowVector, ListColumnVector hiveVector, ListTypeInfo typeInfo) {
    final int size = arrowVector.getValueCount();
    hiveVector.ensureSize(size, false);
    final ArrowBuf offsets = arrowVector.getOffsetBuffer();
    final int OFFSET_WIDTH = 4;

    read(arrowVector.getChildrenFromFields().get(0),
        hiveVector.child,
        typeInfo.getListElementTypeInfo());

    for (int i = 0; i < size; i++) {
      if (arrowVector.isNull(i)) {
        VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
      } else {
        hiveVector.isNull[i] = false;
        final int offset = offsets.getInt(i * OFFSET_WIDTH);
        hiveVector.offsets[i] = offset;
        hiveVector.lengths[i] = offsets.getInt((i + 1) * OFFSET_WIDTH) - offset;
      }
    }
  }

  private void readMap(FieldVector arrowVector, MapColumnVector hiveVector, MapTypeInfo typeInfo) {
    final int size = arrowVector.getValueCount();
    hiveVector.ensureSize(size, false);
    final ListTypeInfo mapStructListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector mapStructListVector = toStructListVector(hiveVector);
    final StructColumnVector mapStructVector = (StructColumnVector) mapStructListVector.child;

    read(arrowVector, mapStructListVector, mapStructListTypeInfo);

    hiveVector.isRepeating = mapStructListVector.isRepeating;
    hiveVector.childCount = mapStructListVector.childCount;
    hiveVector.noNulls = mapStructListVector.noNulls;
    hiveVector.keys = mapStructVector.fields[0];
    hiveVector.values = mapStructVector.fields[1];
    System.arraycopy(mapStructListVector.offsets, 0, hiveVector.offsets, 0, size);
    System.arraycopy(mapStructListVector.lengths, 0, hiveVector.lengths, 0, size);
    System.arraycopy(mapStructListVector.isNull, 0, hiveVector.isNull, 0, size);
  }

  private void readStruct(FieldVector arrowVector, StructColumnVector hiveVector, StructTypeInfo typeInfo) {
    final int size = arrowVector.getValueCount();
    hiveVector.ensureSize(size, false);
    final List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
    final int fieldSize = arrowVector.getChildrenFromFields().size();
    for (int i = 0; i < fieldSize; i++) {
      read(arrowVector.getChildrenFromFields().get(i), hiveVector.fields[i], fieldTypeInfos.get(i));
    }

    for (int i = 0; i < size; i++) {
      if (arrowVector.isNull(i)) {
        VectorizedBatchUtil.setNullColIsNullValue(hiveVector, i);
      } else {
        hiveVector.isNull[i] = false;
      }
    }
  }

  private void readUnion(FieldVector arrowVector, UnionColumnVector hiveVector, UnionTypeInfo typeInfo) {
  }
}
