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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
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
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

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

public class Serializer {
  private final int MAX_BUFFERED_ROWS;

  // Hive columns
  private final VectorizedRowBatch vectorizedRowBatch;
  private final VectorAssignRow vectorAssignRow;
  private int batchSize;
  private BufferAllocator allocator;
  private List<TypeInfo> fieldTypeInfos;
  private List<String> fieldNames;
  private int fieldSize;

  private final NullableMapVector rootVector;

  //Constructor for non-serde serialization
  public Serializer(Configuration conf, String attemptId, List<TypeInfo> typeInfos, List<String> fieldNames) {
    this.fieldTypeInfos = typeInfos;
    this.fieldNames = fieldNames;
    long childAllocatorLimit = HiveConf.getLongVar(conf, HIVE_ARROW_BATCH_ALLOCATOR_LIMIT);
    //Use per-task allocator for accounting only, no need to reserve per-task memory
    long childAllocatorReservation = 0L;
    //Break out accounting of direct memory per-task, so we can check no memory is leaked when task is completed
    allocator = RootAllocatorFactory.INSTANCE.getRootAllocator(conf).newChildAllocator(
        attemptId,
        childAllocatorReservation,
        childAllocatorLimit);
    rootVector = NullableMapVector.empty(null, allocator);
    //These last fields are unused in non-serde usage
    vectorizedRowBatch = null;
    vectorAssignRow = null;
    MAX_BUFFERED_ROWS = 0;
  }

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
    StructTypeInfo structTypeInfo = (StructTypeInfo) getTypeInfoFromObjectInspector(serDe.rowObjectInspector);
    fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    fieldNames = structTypeInfo.getAllStructFieldNames();
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

  //Construct an emptyBatch which contains schema-only info
  public ArrowWrapperWritable emptyBatch() {
    rootVector.setValueCount(0);
    for (int fieldIndex = 0; fieldIndex < fieldTypeInfos.size(); fieldIndex++) {
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
      final String fieldName = fieldNames.get(fieldIndex);
      final FieldType fieldType = toFieldType(fieldTypeInfo);
      final FieldVector arrowVector = rootVector.addOrGet(fieldName, fieldType, FieldVector.class);
      arrowVector.setInitialCapacity(0);
      arrowVector.allocateNew();
    }
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(rootVector);
    return new ArrowWrapperWritable(vectorSchemaRoot, allocator, rootVector);
  }

  //Used for both:
  //1. VectorizedRowBatch constructed by batching rows
  //2. VectorizedRowBatch provided from upstream (isNative)
  public ArrowWrapperWritable serializeBatch(VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    rootVector.setValueCount(0);

    for (int fieldIndex = 0; fieldIndex < vectorizedRowBatch.projectionSize; fieldIndex++) {
      final int projectedColumn = vectorizedRowBatch.projectedColumns[fieldIndex];
      final ColumnVector hiveVector = vectorizedRowBatch.cols[projectedColumn];
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
      final String fieldName = fieldNames.get(fieldIndex);
      final FieldType fieldType = toFieldType(fieldTypeInfo);
      //Reuse existing FieldVector buffers
      //since we always call setValue or setNull for each row
      boolean fieldExists = false;
      if(rootVector.getChild(fieldName) != null) {
        fieldExists = true;
      }
      final FieldVector arrowVector = rootVector.addOrGet(fieldName, fieldType, FieldVector.class);
      if(fieldExists) {
        arrowVector.setValueCount(isNative ? vectorizedRowBatch.size : batchSize);
      } else {
        arrowVector.setInitialCapacity(isNative ? vectorizedRowBatch.size : batchSize);
        arrowVector.allocateNew();
      }
      write(arrowVector, hiveVector, fieldTypeInfo, isNative ? vectorizedRowBatch.size : batchSize, vectorizedRowBatch, isNative);
    }
    if(!isNative) {
      //Only mutate batches that are constructed by this serde
      vectorizedRowBatch.reset();
      rootVector.setValueCount(batchSize);
    } else {
      rootVector.setValueCount(vectorizedRowBatch.size);
    }

    batchSize = 0;
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(rootVector);
    return new ArrowWrapperWritable(vectorSchemaRoot, allocator, rootVector);
  }

  private static FieldType toFieldType(TypeInfo typeInfo) {
    return new FieldType(true, toArrowType(typeInfo), null);
  }

  private static ArrowType toArrowType(TypeInfo typeInfo) {
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

  private static void write(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo, int size,
      VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        writePrimitive(arrowVector, hiveVector, typeInfo, size, vectorizedRowBatch, isNative);
        break;
      case LIST:
        writeList((ListVector) arrowVector, (ListColumnVector) hiveVector, (ListTypeInfo) typeInfo, size, vectorizedRowBatch, isNative);
        break;
      case STRUCT:
        writeStruct((MapVector) arrowVector, (StructColumnVector) hiveVector, (StructTypeInfo) typeInfo, size, vectorizedRowBatch, isNative);
        break;
      case UNION:
        writeUnion(arrowVector, hiveVector, typeInfo, size, vectorizedRowBatch, isNative);
        break;
      case MAP:
        writeMap((ListVector) arrowVector, (MapColumnVector) hiveVector, (MapTypeInfo) typeInfo, size, vectorizedRowBatch, isNative);
        break;
      default:
        throw new IllegalArgumentException();
      }
  }

  private static void writeMap(ListVector arrowVector, MapColumnVector hiveVector, MapTypeInfo typeInfo,
      int size, VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    final ListTypeInfo structListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector structListVector = toStructListVector(hiveVector);

    write(arrowVector, structListVector, structListTypeInfo, size, vectorizedRowBatch, isNative);

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }
  }

  private static void writeUnion(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size, VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
    final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
    final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
    final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;

    final int tag = hiveUnionVector.tags[0];
    final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
    final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);

    write(arrowVector, hiveObjectVector, objectTypeInfo, size, vectorizedRowBatch, isNative);
  }

  private static void writeStruct(MapVector arrowVector, StructColumnVector hiveVector,
      StructTypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
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
      write(arrowFieldVector, hiveFieldVector, fieldTypeInfo, size, vectorizedRowBatch, isNative);
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

  private static void writeList(ListVector arrowVector, ListColumnVector hiveVector, ListTypeInfo typeInfo, int size,
      VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    final int OFFSET_WIDTH = 4;
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final ColumnVector hiveElementVector = hiveVector.child;
    final FieldVector arrowElementVector =
        (FieldVector) arrowVector.addOrGetVector(toFieldType(elementTypeInfo)).getVector();
    arrowElementVector.setInitialCapacity(hiveVector.childCount);
    arrowElementVector.allocateNew();

    write(arrowElementVector, hiveElementVector, elementTypeInfo, hiveVector.childCount, vectorizedRowBatch, isNative);

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

  //Handle cases for both internally constructed
  //and externally provided (isNative) VectorRowBatch
  private static void writePrimitive(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo, int size,
      VectorizedRowBatch vectorizedRowBatch, boolean isNative) {
    final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    switch (primitiveCategory) {
    case BOOLEAN:
    {
      if(isNative) {
      writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, boolNullSetter, boolValueSetter);
        return;
      }
      final BitVector bitVector = (BitVector) arrowVector;
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          boolNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          boolValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case BYTE:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, byteNullSetter, byteValueSetter);
        return;
      }
      final TinyIntVector tinyIntVector = (TinyIntVector) arrowVector;
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          byteNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          byteValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case SHORT:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, shortNullSetter, shortValueSetter);
        return;
      }
      final SmallIntVector smallIntVector = (SmallIntVector) arrowVector;
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          shortNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          shortValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case INT:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, intNullSetter, intValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          intNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          intValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case LONG:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, longNullSetter, longValueSetter);
        return;
      }
      final BigIntVector bigIntVector = (BigIntVector) arrowVector;
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          longNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          longValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case FLOAT:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, floatNullSetter, floatValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          floatNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          floatValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case DOUBLE:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, doubleNullSetter, doubleValueSetter);
        return;
      }
      final Float8Vector float8Vector = (Float8Vector) arrowVector;
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          doubleNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          doubleValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    //TODO Add CHAR padding conversion
    case STRING:
    case VARCHAR:
    case CHAR:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, stringNullSetter, stringValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          stringNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          stringValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case DATE:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, dateNullSetter, dateValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          dateNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          dateValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case TIMESTAMP:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, timestampNullSetter, timestampValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          timestampNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          timestampValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case BINARY:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, binaryNullSetter, binaryValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          binaryNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          binaryValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case DECIMAL:
    {
      if(isNative) {
        if(hiveVector instanceof DecimalColumnVector) {
          writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, decimalNullSetter, decimalValueSetter);
        } else {
          writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, decimalNullSetter, decimal64ValueSetter);
        }
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          decimalNullSetter.accept(i, arrowVector, hiveVector);
        } else if(hiveVector instanceof DecimalColumnVector) {
          decimalValueSetter.accept(i, i, arrowVector, hiveVector);
        } else if(hiveVector instanceof Decimal64ColumnVector) {
          decimal64ValueSetter.accept(i, i, arrowVector, hiveVector);
        } else {
          throw new IllegalArgumentException("Unsupported vector column type: " + hiveVector.getClass().getName());
        }
      }
    }
    break;
    case INTERVAL_YEAR_MONTH:
    {
      if(isNative) {
       writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, intervalYearMonthNullSetter, intervalYearMonthValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          intervalYearMonthNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          intervalYearMonthValueSetter.accept(i, i, arrowVector, hiveVector);
        }
      }
    }
    break;
    case INTERVAL_DAY_TIME:
    {
      if(isNative) {
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch.selectedInUse, vectorizedRowBatch.selected, intervalDayTimeNullSetter, intervalDayTimeValueSetter);
        return;
      }
      for (int i = 0; i < size; i++) {
        if (hiveVector.isNull[i]) {
          intervalDayTimeNullSetter.accept(i, arrowVector, hiveVector);
        } else {
          intervalDayTimeValueSetter.accept(i, i, arrowVector, hiveVector);
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
      return serializeBatch(vectorizedRowBatch, false);
    }
    List<Object> standardObjects = new ArrayList<Object>();
    ObjectInspectorUtils.copyToStandardObject(standardObjects, obj,
        ((StructObjectInspector) objInspector), WRITABLE);

    vectorAssignRow.assignRow(vectorizedRowBatch, batchSize, standardObjects, fieldSize);
    batchSize++;
    if (batchSize == MAX_BUFFERED_ROWS) {
      return serializeBatch(vectorizedRowBatch, false);
    }
    return null;
  }

 //Use a provided nullSetter and valueSetter function to populate
 //fieldVector from hiveVector
 private static void writeGeneric(final FieldVector fieldVector, final ColumnVector hiveVector, final int size, final boolean selectedInUse, final int[] selected, final IntAndVectorsConsumer nullSetter, final IntIntAndVectorsConsumer valueSetter)
  {
     final boolean[] inputIsNull = hiveVector.isNull;
     final int[] sel = selected;

     if (hiveVector.isRepeating) {
       if (hiveVector.noNulls || !inputIsNull[0]) {
         for(int i = 0; i < size; i++) {
           //Fill n rows with value in row 0
           valueSetter.accept(i, 0, fieldVector, hiveVector);
         }
       } else {
         for(int i = 0; i < size; i++) {
           //Fill n rows with NULL
           nullSetter.accept(i, fieldVector, hiveVector);
         }
       }
       return;
     }

     if (hiveVector.noNulls) {
       if (selectedInUse) {
         for(int logical = 0; logical < size; logical++) {
           final int batchIndex = sel[logical];
           //Add row batchIndex
           valueSetter.accept(logical, batchIndex, fieldVector, hiveVector);
         }
       } else {
         for(int batchIndex = 0; batchIndex < size; batchIndex++) {
           //Add row batchIndex
           valueSetter.accept(batchIndex, batchIndex, fieldVector, hiveVector);
         }
       }
     } else {
       if (selectedInUse) {
         for(int logical = 0; logical < size; logical++) {
           final int batchIndex = sel[logical];
           if (inputIsNull[batchIndex]) {
             //Add NULL
             nullSetter.accept(batchIndex, fieldVector, hiveVector);
           } else {
             //Add row batchIndex
             valueSetter.accept(logical, batchIndex, fieldVector, hiveVector);
          }
        }
       } else {
         for(int batchIndex = 0; batchIndex < size; batchIndex++) {
           if (inputIsNull[batchIndex]) {
             //Add NULL
             nullSetter.accept(batchIndex, fieldVector, hiveVector);
           } else {
             //Add row batchIndex
             valueSetter.accept(batchIndex, batchIndex, fieldVector, hiveVector);
         }
       }
     }
   }
  }

  //nullSetters and valueSetter for each type

  //bool
  private static final IntAndVectorsConsumer boolNullSetter = (i, arrowVector, hiveVector)
      -> ((BitVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer boolValueSetter = (i, j, arrowVector, hiveVector)
      -> ((BitVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //byte
  private static final IntAndVectorsConsumer byteNullSetter = (i, arrowVector, hiveVector)
      -> ((TinyIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer byteValueSetter = (i, j, arrowVector, hiveVector)
      -> ((TinyIntVector) arrowVector).set(i, (byte) ((LongColumnVector) hiveVector).vector[j]);

  //short
  private static final IntAndVectorsConsumer shortNullSetter = (i, arrowVector, hiveVector)
      -> ((SmallIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer shortValueSetter = (i, j, arrowVector, hiveVector)
      -> ((SmallIntVector) arrowVector).set(i, (short) ((LongColumnVector) hiveVector).vector[j]);

  //int
  private static final IntAndVectorsConsumer intNullSetter = (i, arrowVector, hiveVector)
      -> ((IntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer intValueSetter = (i, j, arrowVector, hiveVector)
      -> ((IntVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //long
  private static final IntAndVectorsConsumer longNullSetter = (i, arrowVector, hiveVector)
      -> ((BigIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer longValueSetter = (i, j, arrowVector, hiveVector)
      -> ((BigIntVector) arrowVector).set(i, ((LongColumnVector) hiveVector).vector[j]);

  //float
  private static final IntAndVectorsConsumer floatNullSetter = (i, arrowVector, hiveVector)
      -> ((Float4Vector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer floatValueSetter = (i, j, arrowVector, hiveVector)
      -> ((Float4Vector) arrowVector).set(i, (float) ((DoubleColumnVector) hiveVector).vector[j]);

  //double
  private static final IntAndVectorsConsumer doubleNullSetter = (i, arrowVector, hiveVector)
      -> ((Float8Vector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer doubleValueSetter = (i, j, arrowVector, hiveVector)
      -> ((Float8Vector) arrowVector).set(i, ((DoubleColumnVector) hiveVector).vector[j]);

  //string/varchar
  private static final IntAndVectorsConsumer stringNullSetter = (i, arrowVector, hiveVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer stringValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarCharVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //fixed-length CHAR
  //TODO Add padding conversion
  private static final IntAndVectorsConsumer charNullSetter = (i, arrowVector, hiveVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer charValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarCharVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //date
  private static final IntAndVectorsConsumer dateNullSetter = (i, arrowVector, hiveVector)
      -> ((DateDayVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer dateValueSetter = (i, j, arrowVector, hiveVector)
      -> ((DateDayVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //timestamp
  private static final IntAndVectorsConsumer timestampNullSetter = (i, arrowVector, hiveVector)
      -> ((TimeStampMicroTZVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer timestampValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    final TimeStampMicroTZVector timeStampMicroTZVector = (TimeStampMicroTZVector) arrowVector;
    final TimestampColumnVector timestampColumnVector = (TimestampColumnVector) hiveVector;
    // Time = second + sub-second
    final long secondInMillis = timestampColumnVector.getTime(j);
    final long secondInMicros = (secondInMillis - secondInMillis % MILLIS_PER_SECOND) * MICROS_PER_MILLIS;
    final long subSecondInMicros = timestampColumnVector.getNanos(j) / NS_PER_MICROS;
    if ((secondInMillis > 0 && secondInMicros < 0) || (secondInMillis < 0 && secondInMicros > 0)) {
      // If the timestamp cannot be represented in long microsecond, set it as a null value
      timeStampMicroTZVector.setNull(i);
    } else {
      timeStampMicroTZVector.set(i, secondInMicros + subSecondInMicros);
    }
  };

  //binary
  private static final IntAndVectorsConsumer binaryNullSetter = (i, arrowVector, hiveVector)
      -> ((VarBinaryVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer binaryValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarBinaryVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //decimal and decimal64
  private static final IntAndVectorsConsumer decimalNullSetter = (i, arrowVector, hiveVector)
      -> ((DecimalVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer decimalValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    final DecimalVector decimalVector = (DecimalVector) arrowVector;
    final int scale = decimalVector.getScale();
    decimalVector.set(i, ((DecimalColumnVector) hiveVector).vector[j].getHiveDecimal().bigDecimalValue().setScale(scale));
  };
  private static final IntIntAndVectorsConsumer decimal64ValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    final DecimalVector decimalVector = (DecimalVector) arrowVector;
    final int scale = decimalVector.getScale();
    HiveDecimalWritable decimalHolder = new HiveDecimalWritable();
    decimalHolder.setFromLongAndScale(((Decimal64ColumnVector) hiveVector).vector[j], scale);
    decimalVector.set(i, decimalHolder.getHiveDecimal().bigDecimalValue().setScale(scale));
  };

  //interval year
  private static final IntAndVectorsConsumer intervalYearMonthNullSetter = (i, arrowVector, hiveVector)
      -> ((IntervalYearVector) arrowVector).setNull(i);
  private static IntIntAndVectorsConsumer intervalYearMonthValueSetter = (i, j, arrowVector, hiveVector)
      -> ((IntervalYearVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //interval day
  private static final IntAndVectorsConsumer intervalDayTimeNullSetter = (i, arrowVector, hiveVector)
      -> ((IntervalDayVector) arrowVector).setNull(i);
  private static IntIntAndVectorsConsumer intervalDayTimeValueSetter = (i, j, arrowVector, hiveVector)
      -> {
    final IntervalDayVector intervalDayVector = (IntervalDayVector) arrowVector;
    final IntervalDayTimeColumnVector intervalDayTimeColumnVector =
        (IntervalDayTimeColumnVector) hiveVector;
    long totalSeconds = intervalDayTimeColumnVector.getTotalSeconds(j);
    final long days = totalSeconds / SECOND_PER_DAY;
    final long millis =
        (totalSeconds - days * SECOND_PER_DAY) * MILLIS_PER_SECOND +
            intervalDayTimeColumnVector.getNanos(j) / NS_PER_MILLIS;
    intervalDayVector.set(i, (int) days, (int) millis);
  };

  //Used for setting null at arrowVector[i]
  private interface IntAndVectorsConsumer {
    void accept(int i, FieldVector arrowVector, ColumnVector hiveVector);
  }

  //Used to copy value from hiveVector[j] -> arrowVector[i]
  //since hiveVector might be referenced through vector.selected
  private interface IntIntAndVectorsConsumer {
    void accept(int i, int j, FieldVector arrowVector, ColumnVector hiveVector);
  }

}
