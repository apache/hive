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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorAssignRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.arrow.memory.BufferAllocator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARROW_BATCH_SIZE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARROW_BATCH_ALLOCATOR_LIMIT;
import static org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil.createColumnVector;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.MICROS_PER_MILLIS;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.MILLIS_PER_SECOND;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.NS_PER_MICROS;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.NS_PER_MILLIS;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.SECOND_PER_DAY;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.toStructListTypeInfo;
import static org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe.toStructListVector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromObjectInspector;

class Serializer {
  private final int MAX_BUFFERED_ROWS;

  // Schema
  private final StructTypeInfo structTypeInfo;
  private final int fieldSize;

  // Hive columns
  private final VectorizedRowBatch vectorizedRowBatch;
  private final VectorAssignRow vectorAssignRow;
  private int batchSize;
  private BufferAllocator allocator;

  private final NullableMapVector rootVector;

  Serializer(ArrowColumnarBatchSerDe serDe) throws SerDeException {
    MAX_BUFFERED_ROWS = HiveConf.getIntVar(serDe.conf, HIVE_ARROW_BATCH_SIZE);
    long childAllocatorLimit = HiveConf.getLongVar(serDe.conf, HIVE_ARROW_BATCH_ALLOCATOR_LIMIT);
    ArrowColumnarBatchSerDe.LOG.info("ArrowColumnarBatchSerDe max number of buffered columns: " + MAX_BUFFERED_ROWS);
    String childAllocatorName = Thread.currentThread().getName();
    //Use per-task allocator for accounting only, no need to reserve per-task memory
    long childAllocatorReservation = 0L;
    //Break out accounting of direct memory per-task, so we can check no memory is leaked when task is completed
    allocator = serDe.rootAllocator.newChildAllocator(
       childAllocatorName,
       childAllocatorReservation,
       childAllocatorLimit);

    // Schema
    structTypeInfo = (StructTypeInfo) getTypeInfoFromObjectInspector(serDe.rowObjectInspector);
    List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    fieldSize = fieldTypeInfos.size();
    // Init Arrow stuffs
    rootVector = NullableMapVector.empty(null, allocator);

    // Init Hive stuffs
    vectorizedRowBatch = new VectorizedRowBatch(fieldSize);
    for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
      final ColumnVector columnVector = createColumnVector(fieldTypeInfos.get(fieldIndex));
      vectorizedRowBatch.cols[fieldIndex] = columnVector;
      columnVector.init();
    }
    vectorizedRowBatch.ensureSize(MAX_BUFFERED_ROWS);
    vectorAssignRow = new VectorAssignRow();
    try {
      vectorAssignRow.init(serDe.rowObjectInspector);
    } catch (HiveException e) {
      throw new SerDeException(e);
    }
  }

  private ArrowWrapperWritable serializeBatch() {
    rootVector.setValueCount(0);

    for (int fieldIndex = 0; fieldIndex < vectorizedRowBatch.projectionSize; fieldIndex++) {
      final int projectedColumn = vectorizedRowBatch.projectedColumns[fieldIndex];
      final ColumnVector hiveVector = vectorizedRowBatch.cols[projectedColumn];
      final TypeInfo fieldTypeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
      final String fieldName = structTypeInfo.getAllStructFieldNames().get(fieldIndex);
      final FieldType fieldType = toFieldType(fieldTypeInfo);
      final FieldVector arrowVector = rootVector.addOrGet(fieldName, fieldType, FieldVector.class);
      arrowVector.setInitialCapacity(batchSize);
      arrowVector.allocateNew();
      write(arrowVector, hiveVector, fieldTypeInfo, batchSize);
    }
    vectorizedRowBatch.reset();
    rootVector.setValueCount(batchSize);

    batchSize = 0;
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(rootVector);
    return new ArrowWrapperWritable(vectorSchemaRoot, allocator, rootVector);
  }

  private FieldType toFieldType(TypeInfo typeInfo) {
    return new FieldType(true, toArrowType(typeInfo), null);
  }

  private ArrowType toArrowType(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
          case BOOLEAN:
            return Types.MinorType.BIT.getType();
          case BYTE:
            return Types.MinorType.TINYINT.getType();
          case SHORT:
            return Types.MinorType.SMALLINT.getType();
          case INT:
            return Types.MinorType.INT.getType();
          case LONG:
            return Types.MinorType.BIGINT.getType();
          case FLOAT:
            return Types.MinorType.FLOAT4.getType();
          case DOUBLE:
            return Types.MinorType.FLOAT8.getType();
          case STRING:
          case VARCHAR:
          case CHAR:
            return Types.MinorType.VARCHAR.getType();
          case DATE:
            return Types.MinorType.DATEDAY.getType();
          case TIMESTAMP:
            // HIVE-19853: Prefer timestamp in microsecond with time zone because Spark supports it
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
          case BINARY:
            return Types.MinorType.VARBINARY.getType();
          case DECIMAL:
            final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            return new ArrowType.Decimal(decimalTypeInfo.precision(), decimalTypeInfo.scale());
          case INTERVAL_YEAR_MONTH:
            return Types.MinorType.INTERVALYEAR.getType();
          case INTERVAL_DAY_TIME:
            return Types.MinorType.INTERVALDAY.getType();
          case VOID:
          case TIMESTAMPLOCALTZ:
          case UNKNOWN:
          default:
            throw new IllegalArgumentException();
        }
      case LIST:
        return ArrowType.List.INSTANCE;
      case STRUCT:
        return ArrowType.Struct.INSTANCE;
      case MAP:
        return ArrowType.List.INSTANCE;
      case UNION:
      default:
        throw new IllegalArgumentException();
    }
  }

  private void write(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo, int size) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        writePrimitive(arrowVector, hiveVector, typeInfo, size);
        break;
      case LIST:
        writeList((ListVector) arrowVector, (ListColumnVector) hiveVector, (ListTypeInfo) typeInfo, size);
        break;
      case STRUCT:
        writeStruct((MapVector) arrowVector, (StructColumnVector) hiveVector, (StructTypeInfo) typeInfo, size);
        break;
      case UNION:
        writeUnion(arrowVector, hiveVector, typeInfo, size);
        break;
      case MAP:
        writeMap((ListVector) arrowVector, (MapColumnVector) hiveVector, (MapTypeInfo) typeInfo, size);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  private void writeMap(ListVector arrowVector, MapColumnVector hiveVector, MapTypeInfo typeInfo,
      int size) {
    final ListTypeInfo structListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector structListVector = toStructListVector(hiveVector);

    write(arrowVector, structListVector, structListTypeInfo, size);

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }
  }

  private void writeUnion(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size) {
    final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
    final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
    final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
    final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;

    final int tag = hiveUnionVector.tags[0];
    final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
    final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);

    write(arrowVector, hiveObjectVector, objectTypeInfo, size);
  }

  private void writeStruct(MapVector arrowVector, StructColumnVector hiveVector,
      StructTypeInfo typeInfo, int size) {
    final List<String> fieldNames = typeInfo.getAllStructFieldNames();
    final List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
    final ColumnVector[] hiveFieldVectors = hiveVector.fields;
    final int fieldSize = fieldTypeInfos.size();

    for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
      final ColumnVector hiveFieldVector = hiveFieldVectors[fieldIndex];
      final String fieldName = fieldNames.get(fieldIndex);
      final FieldVector arrowFieldVector =
          arrowVector.addOrGet(fieldName,
              toFieldType(fieldTypeInfos.get(fieldIndex)), FieldVector.class);
      arrowFieldVector.setInitialCapacity(size);
      arrowFieldVector.allocateNew();
      write(arrowFieldVector, hiveFieldVector, fieldTypeInfo, size);
    }

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }
  }

  private void writeList(ListVector arrowVector, ListColumnVector hiveVector, ListTypeInfo typeInfo,
      int size) {
    final int OFFSET_WIDTH = 4;
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final ColumnVector hiveElementVector = hiveVector.child;
    final FieldVector arrowElementVector =
        (FieldVector) arrowVector.addOrGetVector(toFieldType(elementTypeInfo)).getVector();
    arrowElementVector.setInitialCapacity(hiveVector.childCount);
    arrowElementVector.allocateNew();

    write(arrowElementVector, hiveElementVector, elementTypeInfo, hiveVector.childCount);

    final ArrowBuf offsetBuffer = arrowVector.getOffsetBuffer();
    int nextOffset = 0;

    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        offsetBuffer.setInt(rowIndex * OFFSET_WIDTH, nextOffset);
      } else {
        offsetBuffer.setInt(rowIndex * OFFSET_WIDTH, nextOffset);
        nextOffset += (int) hiveVector.lengths[rowIndex];
        arrowVector.setNotNull(rowIndex);
      }
    }
    offsetBuffer.setInt(size * OFFSET_WIDTH, nextOffset);
  }

  private void writePrimitive(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size) {
    final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    switch (primitiveCategory) {
      case BOOLEAN:
        {
          final BitVector bitVector = (BitVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              bitVector.setNull(i);
            } else {
              bitVector.set(i, (int) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case BYTE:
        {
          final TinyIntVector tinyIntVector = (TinyIntVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              tinyIntVector.setNull(i);
            } else {
              tinyIntVector.set(i, (byte) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case SHORT:
        {
          final SmallIntVector smallIntVector = (SmallIntVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              smallIntVector.setNull(i);
            } else {
              smallIntVector.set(i, (short) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case INT:
        {
          final IntVector intVector = (IntVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              intVector.setNull(i);
            } else {
              intVector.set(i, (int) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case LONG:
        {
          final BigIntVector bigIntVector = (BigIntVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              bigIntVector.setNull(i);
            } else {
              bigIntVector.set(i, ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case FLOAT:
        {
          final Float4Vector float4Vector = (Float4Vector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              float4Vector.setNull(i);
            } else {
              float4Vector.set(i, (float) ((DoubleColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case DOUBLE:
        {
          final Float8Vector float8Vector = (Float8Vector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              float8Vector.setNull(i);
            } else {
              float8Vector.set(i, ((DoubleColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case STRING:
      case VARCHAR:
      case CHAR:
        {
          final VarCharVector varCharVector = (VarCharVector) arrowVector;
          final BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              varCharVector.setNull(i);
            } else {
              varCharVector.setSafe(i, bytesVector.vector[i], bytesVector.start[i], bytesVector.length[i]);
            }
          }
        }
        break;
      case DATE:
        {
          final DateDayVector dateDayVector = (DateDayVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              dateDayVector.setNull(i);
            } else {
              dateDayVector.set(i, (int) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case TIMESTAMP:
        {
          final TimeStampMicroTZVector timeStampMicroTZVector = (TimeStampMicroTZVector) arrowVector;
          final TimestampColumnVector timestampColumnVector = (TimestampColumnVector) hiveVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              timeStampMicroTZVector.setNull(i);
            } else {
              // Time = second + sub-second
              final long secondInMillis = timestampColumnVector.getTime(i);
              final long secondInMicros = (secondInMillis - secondInMillis % MILLIS_PER_SECOND) * MICROS_PER_MILLIS;
              final long subSecondInMicros = timestampColumnVector.getNanos(i) / NS_PER_MICROS;

              if ((secondInMillis > 0 && secondInMicros < 0) || (secondInMillis < 0 && secondInMicros > 0)) {
                // If the timestamp cannot be represented in long microsecond, set it as a null value
                timeStampMicroTZVector.setNull(i);
              } else {
                timeStampMicroTZVector.set(i, secondInMicros + subSecondInMicros);
              }
            }
          }
        }
        break;
      case BINARY:
        {
          final VarBinaryVector varBinaryVector = (VarBinaryVector) arrowVector;
          final BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              varBinaryVector.setNull(i);
            } else {
              varBinaryVector.setSafe(i, bytesVector.vector[i], bytesVector.start[i], bytesVector.length[i]);
            }
          }
        }
        break;
      case DECIMAL:
        {
          final DecimalVector decimalVector = (DecimalVector) arrowVector;
          final int scale = decimalVector.getScale();
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              decimalVector.setNull(i);
            } else {
              decimalVector.set(i,
                  ((DecimalColumnVector) hiveVector).vector[i].getHiveDecimal().bigDecimalValue().setScale(scale));
            }
          }
        }
        break;
      case INTERVAL_YEAR_MONTH:
        {
          final IntervalYearVector intervalYearVector = (IntervalYearVector) arrowVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              intervalYearVector.setNull(i);
            } else {
              intervalYearVector.set(i, (int) ((LongColumnVector) hiveVector).vector[i]);
            }
          }
        }
        break;
      case INTERVAL_DAY_TIME:
        {
          final IntervalDayVector intervalDayVector = (IntervalDayVector) arrowVector;
          final IntervalDayTimeColumnVector intervalDayTimeColumnVector =
              (IntervalDayTimeColumnVector) hiveVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              intervalDayVector.setNull(i);
            } else {
              final long totalSeconds = intervalDayTimeColumnVector.getTotalSeconds(i);
              final long days = totalSeconds / SECOND_PER_DAY;
              final long millis =
                  (totalSeconds - days * SECOND_PER_DAY) * MILLIS_PER_SECOND +
                      intervalDayTimeColumnVector.getNanos(i) / NS_PER_MILLIS;
              intervalDayVector.set(i, (int) days, (int) millis);
            }
          }
        }
        break;
      case VOID:
      case UNKNOWN:
      case TIMESTAMPLOCALTZ:
      default:
        throw new IllegalArgumentException();
    }
  }

  ArrowWrapperWritable serialize(Object obj, ObjectInspector objInspector) {
    // if row is null, it means there are no more rows (closeOp()).
    // another case can be that the buffer is full.
    if (obj == null) {
      return serializeBatch();
    }
    List<Object> standardObjects = new ArrayList<Object>();
    ObjectInspectorUtils.copyToStandardObject(standardObjects, obj,
        ((StructObjectInspector) objInspector), WRITABLE);

    vectorAssignRow.assignRow(vectorizedRowBatch, batchSize, standardObjects, fieldSize);
    batchSize++;
    if (batchSize == MAX_BUFFERED_ROWS) {
      return serializeBatch();
    }
    return null;
  }
}
