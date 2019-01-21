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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang.mutable.MutableInt;
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
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.arrow.memory.BufferAllocator;

import java.math.BigDecimal;
import java.math.BigInteger;
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
  private final static byte[] EMPTY_BYTES = new byte[0];

  // Hive columns
  private final VectorizedRowBatch vectorizedRowBatch;
  private final VectorAssignRow vectorAssignRow;
  private int batchSize;
  private BufferAllocator allocator;
  private List<TypeInfo> fieldTypeInfos;
  private List<String> fieldNames;
  private int fieldSize;

  private final StructVector rootVector;
  private final DecimalHolder decimalHolder = new DecimalHolder();

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
    rootVector = StructVector.empty(null, allocator);
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
    rootVector = StructVector.empty(null, allocator);

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
  public ArrowWrapperWritable serializeBatch(VectorizedRowBatch vectorizedRowBatch,
      boolean isNative) {
    rootVector.setValueCount(0);

    final int size = isNative ? vectorizedRowBatch.size : batchSize;
    for (int fieldIndex = 0; fieldIndex < vectorizedRowBatch.projectionSize; fieldIndex++) {
      final int projectedColumn = vectorizedRowBatch.projectedColumns[fieldIndex];
      final ColumnVector hiveVector = vectorizedRowBatch.cols[projectedColumn];
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
      final String fieldName = fieldNames.get(fieldIndex);
      final FieldType fieldType = toFieldType(fieldTypeInfo);
      //Reuse existing FieldVector buffers
      //since we always call setValue or setNull for each row
      final FieldVector arrowVector = rootVector.addOrGet(fieldName, fieldType, FieldVector.class);
      arrowVector.setInitialCapacity(size);
      arrowVector.allocateNew();
      write(arrowVector, hiveVector, fieldTypeInfo, size, vectorizedRowBatch.selectedInUse,
          vectorizedRowBatch.selected);
    }
    if(!isNative) {
      //Only mutate batches that are constructed by this serde
      vectorizedRowBatch.reset();
    }
    rootVector.setValueCount(size);

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

  private void write(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo, int size,
      boolean selectedInUse, int[] selected) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        writePrimitive(arrowVector, hiveVector, typeInfo, size, selectedInUse, selected);
        break;
      case LIST:
        writeList((ListVector) arrowVector, (ListColumnVector) hiveVector, (ListTypeInfo)
          typeInfo, size, selectedInUse, selected);
        break;
      case STRUCT:
        writeStruct((NonNullableStructVector) arrowVector, (StructColumnVector) hiveVector,
            (StructTypeInfo) typeInfo, size, selectedInUse, selected);
        break;
      case UNION:
        writeUnion(arrowVector, hiveVector, typeInfo, size, selectedInUse, selected);
        break;
      case MAP:
        writeMap((ListVector) arrowVector, (MapColumnVector) hiveVector, (MapTypeInfo) typeInfo,
            size, selectedInUse, selected);
        break;
      default:
        throw new IllegalArgumentException();
      }
  }

  private void writeMap(ListVector arrowVector, MapColumnVector hiveVector, MapTypeInfo typeInfo,
      int size, boolean selectedInUse, int[] selected) {
    final ListTypeInfo structListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector structListVector = toStructListVector(hiveVector);

    write(arrowVector, structListVector, structListTypeInfo, size, selectedInUse, selected);

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    final NullSetter nullSetter = (i, x) ->
        BitVectorHelper.setValidityBit(validityBuffer, i, 0);
    final ValueSetter valueSetter = (arrowIndex, hiveIndex, x, y, z) ->
        BitVectorHelper.setValidityBitToOne(validityBuffer, arrowIndex);
    writeGeneric(arrowVector, hiveVector, size, selectedInUse, selected, nullSetter, valueSetter,
        typeInfo);
  }

  private void writeUnion(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size, boolean selectedInUse, int[] selected) {
    final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
    final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
    final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
    final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;

    final int tag = hiveUnionVector.tags[0];
    final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
    final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);

    write(arrowVector, hiveObjectVector, objectTypeInfo, size, selectedInUse, selected);
  }

  private void writeStruct(NonNullableStructVector arrowVector, StructColumnVector hiveVector,
      StructTypeInfo typeInfo, int size, boolean selectedInUse, int[] selected) {
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
      write(arrowFieldVector, hiveFieldVector, fieldTypeInfo, size, selectedInUse, selected);
    }

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    final NullSetter nullSetter = (i, x) ->
        BitVectorHelper.setValidityBit(validityBuffer, i, 0);
    final ValueSetter valueSetter = (arrowIndex, hiveIndex, x, y, z) ->
        BitVectorHelper.setValidityBitToOne(validityBuffer, arrowIndex);
    writeGeneric(arrowVector, hiveVector, size, selectedInUse, selected, nullSetter, valueSetter,
        typeInfo);
  }

  private void writeList(ListVector arrowVector, ListColumnVector hiveVector, ListTypeInfo typeInfo,
      int size, boolean selectedInUse, int[] selected) {
    final int OFFSET_WIDTH = BaseRepeatedValueVector.OFFSET_WIDTH;
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final ColumnVector hiveElementVector = hiveVector.child;
    final FieldVector arrowElementVector =
        (FieldVector) arrowVector.addOrGetVector(toFieldType(elementTypeInfo)).getVector();
    arrowElementVector.setInitialCapacity(hiveVector.childCount);
    arrowElementVector.allocateNew();

    write(arrowElementVector, hiveElementVector, elementTypeInfo, hiveVector.childCount, false,
        null);

    final NullSetter nullSetter = (i, x) -> {};
    final ValueSetter valueSetter = (arrowIndex, hiveIndex, x, y, z) -> {
        arrowVector.startNewValue(arrowIndex);
        arrowVector.endValue(arrowIndex, (int) hiveVector.lengths[hiveIndex]);
    };

    writeGeneric(arrowVector, hiveVector, size, selectedInUse, selected, nullSetter, valueSetter,
        typeInfo);
  }

  //Handle cases for both internally constructed
  //and externally provided (isNative) VectorRowBatch
  private void writePrimitive(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size, boolean selectedInUse, int[] selected) {
    final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    final NullSetter nullSetter;
    final ValueSetter valueSetter;

    switch (primitiveCategory) {
    case BOOLEAN:
      nullSetter = boolNullSetter;
      valueSetter = boolValueSetter;
      break;
    case BYTE:
      nullSetter = byteNullSetter;
      valueSetter = byteValueSetter;
      break;
    case SHORT:
      nullSetter = shortNullSetter;
      valueSetter = shortValueSetter;
      break;
    case INT:
      nullSetter = intNullSetter;
      valueSetter = intValueSetter;
      break;
    case LONG:
      nullSetter = longNullSetter;
      valueSetter = longValueSetter;
      break;
    case FLOAT:
      nullSetter = floatNullSetter;
      valueSetter = floatValueSetter;
      break;
    case DOUBLE:
      nullSetter = doubleNullSetter;
      valueSetter = doubleValueSetter;
      break;
    case CHAR:
      nullSetter = charNullSetter;
      valueSetter = charValueSetter;
      break;
    case STRING:
    case VARCHAR:
      nullSetter = stringNullSetter;
      valueSetter = stringValueSetter;
      break;
    case DATE:
      nullSetter = dateNullSetter;
      valueSetter = dateValueSetter;
      break;
    case TIMESTAMP:
      nullSetter = timestampNullSetter;
      valueSetter = timestampValueSetter;
      break;
    case BINARY:
      nullSetter = binaryNullSetter;
      valueSetter = binaryValueSetter;
      break;
    case DECIMAL:
      if(hiveVector instanceof DecimalColumnVector) {
        nullSetter = decimalNullSetter;
        valueSetter = decimalValueSetter;
      } else {
        nullSetter = decimalNullSetter;
        valueSetter = decimal64ValueSetter;
      }
      break;
    case INTERVAL_YEAR_MONTH:
      nullSetter = intervalYearMonthNullSetter;
      valueSetter = intervalYearMonthValueSetter;
      break;
    case INTERVAL_DAY_TIME:
      nullSetter = intervalDayTimeNullSetter;
      valueSetter = intervalDayTimeValueSetter;
      break;
    case VOID:
    case UNKNOWN:
    case TIMESTAMPLOCALTZ:
    default:
      throw new IllegalArgumentException();
    }

    writeGeneric(arrowVector, hiveVector, size, selectedInUse, selected, nullSetter, valueSetter,
        typeInfo);
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

  // Use a provided nullSetter and valueSetter function to populate
  // arrowVector from hiveVector
  private static void writeGeneric(final ValueVector arrowVector, final ColumnVector hiveVector,
      final int size, final boolean selectedInUse, final int[] selected,
      final NullSetter nullSetter, final ValueSetter valueSetter, final TypeInfo typeInfo) {

    final boolean[] inputIsNull = hiveVector.isNull;
    final int[] sel = selected;

    if (hiveVector.isRepeating) {
      if (hiveVector.noNulls || !inputIsNull[0]) {
        for (int i = 0; i < size; i++) {
          //Fill n rows with value in row 0
          valueSetter.accept(i, 0, arrowVector, hiveVector, typeInfo);
        }
      } else {
        for (int i = 0; i < size; i++) {
          //Fill n rows with NULL
          nullSetter.accept(i, arrowVector);
        }
      }
    } else {
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex;
        if (selectedInUse) {
          batchIndex = sel[logical];
        } else {
          batchIndex = logical;
        }
        if (hiveVector.noNulls || !inputIsNull[batchIndex]) {
          valueSetter.accept(logical, batchIndex, arrowVector, hiveVector, typeInfo);
        } else {
          nullSetter.accept(logical, arrowVector);
        }
      }
    }
    arrowVector.setValueCount(size);
  }

  //bool
  private static final NullSetter boolNullSetter = (i, arrowVector)
      -> ((BitVector) arrowVector).setNull(i);
  private static final ValueSetter boolValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((BitVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //byte
  private static final NullSetter byteNullSetter = (i, arrowVector)
      -> ((TinyIntVector) arrowVector).setNull(i);
  private static final ValueSetter byteValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((TinyIntVector) arrowVector).set(i, (byte) ((LongColumnVector) hiveVector).vector[j]);

  //short
  private static final NullSetter shortNullSetter = (i, arrowVector)
      -> ((SmallIntVector) arrowVector).setNull(i);
  private static final ValueSetter shortValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((SmallIntVector) arrowVector).set(i, (short) ((LongColumnVector) hiveVector).vector[j]);

  //int
  private static final NullSetter intNullSetter = (i, arrowVector)
      -> ((IntVector) arrowVector).setNull(i);
  private static final ValueSetter intValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((IntVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //long
  private static final NullSetter longNullSetter = (i, arrowVector)
      -> ((BigIntVector) arrowVector).setNull(i);
  private static final ValueSetter longValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((BigIntVector) arrowVector).set(i, ((LongColumnVector) hiveVector).vector[j]);

  //float
  private static final NullSetter floatNullSetter = (i, arrowVector)
      -> ((Float4Vector) arrowVector).setNull(i);
  private static final ValueSetter floatValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((Float4Vector) arrowVector).set(i, (float) ((DoubleColumnVector) hiveVector).vector[j]);

  //double
  private static final NullSetter doubleNullSetter = (i, arrowVector)
      -> ((Float8Vector) arrowVector).setNull(i);
  private static final ValueSetter doubleValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((Float8Vector) arrowVector).set(i, ((DoubleColumnVector) hiveVector).vector[j]);

  //string/varchar
  private static final NullSetter stringNullSetter = (i, arrowVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final ValueSetter stringValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarCharVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //fixed-length CHAR
  private static final NullSetter charNullSetter = (i, arrowVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final ValueSetter charValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    VarCharVector varCharVector = (VarCharVector) arrowVector;
    byte[] bytes = bytesVector.vector[j];
    int length = bytesVector.length[j];
    int start = bytesVector.start[j];

    if (bytes == null) {
      bytes = EMPTY_BYTES;
      start = 0;
      length = 0;
    }

    final CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
    final int paddedLength = charTypeInfo.getLength();
    final byte[] paddedBytes = StringExpr.padRight(bytes, start, length, paddedLength);
    varCharVector.setSafe(i, paddedBytes, 0, paddedBytes.length);
  };

  //date
  private static final NullSetter dateNullSetter = (i, arrowVector)
      -> ((DateDayVector) arrowVector).setNull(i);
  private static final ValueSetter dateValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((DateDayVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //timestamp
  private static final NullSetter timestampNullSetter = (i, arrowVector)
      -> ((TimeStampMicroTZVector) arrowVector).setNull(i);
  private static final ValueSetter timestampValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
  private static final NullSetter binaryNullSetter = (i, arrowVector)
      -> ((VarBinaryVector) arrowVector).setNull(i);
  private static final ValueSetter binaryValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarBinaryVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //decimal and decimal64
  private static final NullSetter decimalNullSetter = (i, arrowVector)
      -> ((DecimalVector) arrowVector).setNull(i);
  private final ValueSetter decimalValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    final DecimalVector decimalVector = (DecimalVector) arrowVector;
    final int scale = decimalVector.getScale();
    final HiveDecimalWritable writable = ((DecimalColumnVector) hiveVector).vector[i];
    decimalHolder.precision = writable.precision();
    decimalHolder.scale = scale;
    try (ArrowBuf arrowBuf = allocator.buffer(DecimalHolder.WIDTH)) {
      decimalHolder.buffer = arrowBuf;
      final BigInteger bigInteger = new BigInteger(writable.getInternalStorage()).
          multiply(BigInteger.TEN.pow(scale - writable.scale()));
      decimalVector.set(i, new BigDecimal(bigInteger, scale));
    }
  };
  private static final ValueSetter decimal64ValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    final DecimalVector decimalVector = (DecimalVector) arrowVector;
    final int scale = decimalVector.getScale();
    HiveDecimalWritable decimalHolder = new HiveDecimalWritable();
    decimalHolder.setFromLongAndScale(((Decimal64ColumnVector) hiveVector).vector[j], scale);
    decimalVector.set(i, decimalHolder.getHiveDecimal().bigDecimalValue().setScale(scale));
  };

  //interval year
  private static final NullSetter intervalYearMonthNullSetter = (i, arrowVector)
      -> ((IntervalYearVector) arrowVector).setNull(i);
  private static ValueSetter intervalYearMonthValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((IntervalYearVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //interval day
  private static final NullSetter intervalDayTimeNullSetter = (i, arrowVector)
      -> ((IntervalDayVector) arrowVector).setNull(i);
  private static ValueSetter intervalDayTimeValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
  private interface NullSetter {
    void accept(int i, ValueVector arrowVector);
  }

  //Used to copy value from hiveVector[hiveIndex] -> arrowVector[arrowIndex]
  //since hiveVector might be referenced through vector.selected
  private interface ValueSetter {
    void accept(int arrowIndex, int hiveIndex, ValueVector arrowVector, ColumnVector hiveVector,
        TypeInfo typeInfo);
  }
}
