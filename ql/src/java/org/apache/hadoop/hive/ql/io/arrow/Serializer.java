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
import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
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
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  private final NullableMapVector rootVector;
  private final Configuration conf;
  private final boolean encode;
  private long dictionaryId;
  private DictionaryProvider.MapDictionaryProvider dictionaryProvider;

  //Constructor for non-serde serialization
  public Serializer(Configuration conf, String attemptId, List<TypeInfo> typeInfos, List<String> fieldNames) {
    this.conf = conf;
    encode = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ARROW_ENCODE);

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
    conf = serDe.conf;
    encode = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ARROW_ENCODE);

    long childAllocatorLimit = HiveConf.getLongVar(serDe.conf, HIVE_ARROW_BATCH_ALLOCATOR_LIMIT);
    ArrowColumnarBatchSerDe.LOG.info("ArrowColumnarBatchSerDe max number of buffered columns: " +
        MAX_BUFFERED_ROWS);
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
      final Field field = toField(fieldName, fieldTypeInfo);
      final FieldVector arrowVector = field.createVector(allocator);
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
    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
    final List<Field> fieldList = new ArrayList<>();
    final List<FieldVector> vectorList = new ArrayList<>();
    final int size = isNative ? vectorizedRowBatch.size : batchSize;

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
        arrowVector.setValueCount(size);
      } else {
        arrowVector.setInitialCapacity(size);
        arrowVector.allocateNew();
      }
      vectorList.add(write(arrowVector, hiveVector, fieldTypeInfo, size, vectorizedRowBatch));
      fieldList.add(arrowVector.getField());
      arrowVector.setValueCount(size);
    }
    if(!isNative) {
      //Only mutate batches that are constructed by this serde
      vectorizedRowBatch.reset();
    }
    rootVector.setValueCount(size);

    batchSize = 0;
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fieldList, vectorList, size);
    ArrowWrapperWritable arrowWrapperWritable = new ArrowWrapperWritable(vectorSchemaRoot, allocator, rootVector);
    arrowWrapperWritable.setDictionaryProvider(dictionaryProvider);
    return arrowWrapperWritable;
  }

  private Field toField(String fieldName, TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return new Field(fieldName, toFieldType(typeInfo), null);
      case MAP:
        return toField(fieldName, toStructListTypeInfo((MapTypeInfo) typeInfo));
      case LIST:
      {
        final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        final Field elementField = toField(null, listTypeInfo.getListElementTypeInfo());
        final List<Field> children = Collections.singletonList(elementField);
        return new Field(fieldName, toFieldType(typeInfo), children);
      }
      case STRUCT:
      {
        final List<Field> children = new ArrayList<>();
        final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        final List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        for (int i = 0; i < fieldNames.size(); i++) {
            children.add(toField(fieldNames.get(i), fieldTypeInfos.get(i)));
        }
        return new Field(fieldName, toFieldType(typeInfo), children);
      }
      case UNION:
      default:
        throw new IllegalArgumentException();
    }
  }

  private FieldType toFieldType(TypeInfo typeInfo) {
    if (encode) {
      if (typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
            primitiveTypeInfo.getPrimitiveCategory();
        switch (primitiveCategory) {
          case VARCHAR:
          case CHAR:
          case STRING:
          {
            return new FieldType(true, toArrowType(TypeInfoFactory.intTypeInfo),
                new DictionaryEncoding(dictionaryId++, false, null));
          }
        }
      }
    }
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

  private FieldVector write(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size, VectorizedRowBatch vectorizedRowBatch) {

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return writePrimitive(arrowVector, hiveVector, typeInfo, size, vectorizedRowBatch);
      case LIST:
        return writeList((ListVector) arrowVector, (ListColumnVector) hiveVector,
            (ListTypeInfo) typeInfo, size, vectorizedRowBatch);
      case STRUCT:
        return writeStruct((MapVector) arrowVector, (StructColumnVector) hiveVector,
            (StructTypeInfo) typeInfo, size, vectorizedRowBatch);
      case UNION:
        return writeUnion(arrowVector, hiveVector, typeInfo, size, vectorizedRowBatch);
      case MAP:
        return writeMap((ListVector) arrowVector, (MapColumnVector) hiveVector,
            (MapTypeInfo) typeInfo, size, vectorizedRowBatch);
      default:
        throw new IllegalArgumentException();
      }
  }

  private FieldVector writeMap(ListVector arrowVector, MapColumnVector hiveVector,
      MapTypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch) {

    final ListTypeInfo structListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector structListVector = toStructListVector(hiveVector);

    write(arrowVector, structListVector, structListTypeInfo, size, vectorizedRowBatch);

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }

    return arrowVector;
  }

  private FieldVector writeUnion(FieldVector arrowVector, ColumnVector hiveVector,
      TypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch) {

    final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
    final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
    final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
    final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;

    final int tag = hiveUnionVector.tags[0];
    final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
    final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);

    write(arrowVector, hiveObjectVector, objectTypeInfo, size, vectorizedRowBatch);

    return arrowVector;
  }

  private FieldVector writeStruct(MapVector arrowVector, StructColumnVector hiveVector,
      StructTypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch) {

    final List<String> fieldNames = typeInfo.getAllStructFieldNames();
    final List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
    final ColumnVector[] hiveFieldVectors = hiveVector.fields;
    final int fieldSize = fieldTypeInfos.size();

    for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
      final ColumnVector hiveFieldVector = hiveFieldVectors[fieldIndex];
      final String fieldName = fieldNames.get(fieldIndex);
      final FieldVector arrowFieldVector = arrowVector.addOrGet(fieldName,
          toFieldType(fieldTypeInfos.get(fieldIndex)), FieldVector.class);
      arrowFieldVector.setInitialCapacity(size);
      arrowFieldVector.allocateNew();
      write(arrowFieldVector, hiveFieldVector, fieldTypeInfo, size, vectorizedRowBatch);
    }

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }
    @SuppressWarnings("unchecked")
    FieldVector ret = (FieldVector) arrowVector;
    return ret;
  }

  private FieldVector writeList(ListVector arrowVector, ListColumnVector hiveVector,
      ListTypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch) {

    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final ColumnVector hiveElementVector = hiveVector.child;
    final FieldVector arrowElementVector =
        (FieldVector) arrowVector.addOrGetVector(toFieldType(elementTypeInfo)).getVector();
    arrowElementVector.setInitialCapacity(hiveVector.childCount);
    arrowElementVector.allocateNew();

    write(arrowElementVector, hiveElementVector, elementTypeInfo, hiveVector.childCount,
        vectorizedRowBatch);

    final ArrowBuf offsetBuffer = arrowVector.getOffsetBuffer();
    int nextOffset = 0;

    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        offsetBuffer.setInt(rowIndex * ListVector.OFFSET_WIDTH, nextOffset);
      } else {
        offsetBuffer.setInt(rowIndex * ListVector.OFFSET_WIDTH, nextOffset);
        nextOffset += (int) hiveVector.lengths[rowIndex];
        arrowVector.setNotNull(rowIndex);
      }
    }
    offsetBuffer.setInt(size * ListVector.OFFSET_WIDTH, nextOffset);
    return arrowVector;
  }

  private FieldVector writePrimitive(FieldVector arrowVector, ColumnVector hiveVector,
      TypeInfo typeInfo, int size, VectorizedRowBatch vectorizedRowBatch) {

    final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
    final IntAndVectorsConsumer nullSetter;
    final IntIntAndVectorsConsumer valueSetter;
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
      nullSetter = decimalNullSetter;
      if(hiveVector instanceof DecimalColumnVector) {
        valueSetter = decimalValueSetter;
      } else {
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

    switch (primitiveCategory) {
      case CHAR:
      case STRING:
      case VARCHAR:
        writeString(arrowVector, hiveVector, size, vectorizedRowBatch, nullSetter, valueSetter, typeInfo);
        break;
      default:
        writeGeneric(arrowVector, hiveVector, size, vectorizedRowBatch, nullSetter, valueSetter, typeInfo);
        break;
    }
    return arrowVector;
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
  private static FieldVector writeGeneric(final FieldVector arrowVector, final ColumnVector hiveVector,
      final int size, final VectorizedRowBatch batch,
      final IntAndVectorsConsumer nullSetter, final IntIntAndVectorsConsumer valueSetter,
      TypeInfo typeInfo) {

    final boolean[] inputIsNull = hiveVector.isNull;
    final int[] sel = batch.selected;
    final boolean selectedInUse = batch.selectedInUse;

    if (hiveVector.isRepeating) {
      if (hiveVector.noNulls || !inputIsNull[0]) {
        for(int i = 0; i < size; i++) {
           //Fill n rows with value in row 0
           valueSetter.accept(i, 0, arrowVector, hiveVector, typeInfo);
        }
      } else {
        for(int i = 0; i < size; i++) {
          //Fill n rows with NULL
          nullSetter.accept(i, arrowVector, hiveVector);
        }
      }
      return arrowVector;
    }

    if (hiveVector.noNulls) {
      if (selectedInUse) {
        for(int logical = 0; logical < size; logical++) {
          final int batchIndex = sel[logical];
          //Add row batchIndex
          valueSetter.accept(logical, batchIndex, arrowVector, hiveVector, typeInfo);
        }
      } else {
        for(int batchIndex = 0; batchIndex < size; batchIndex++) {
          //Add row batchIndex
          valueSetter.accept(batchIndex, batchIndex, arrowVector, hiveVector, typeInfo);
        }
      }
    } else {
      if (selectedInUse) {
        for(int logical = 0; logical < size; logical++) {
          final int batchIndex = sel[logical];
          if (inputIsNull[batchIndex]) {
            //Add NULL
            nullSetter.accept(batchIndex, arrowVector, hiveVector);
          } else {
            //Add row batchIndex
            valueSetter.accept(logical, batchIndex, arrowVector, hiveVector, typeInfo);
          }
        }
      } else {
        for (int batchIndex = 0; batchIndex < size; batchIndex++) {
          if (inputIsNull[batchIndex]) {
            //Add NULL
            nullSetter.accept(batchIndex, arrowVector, hiveVector);
          } else {
            //Add row batchIndex
            valueSetter.accept(batchIndex, batchIndex, arrowVector, hiveVector, typeInfo);
          }
        }
      }
    }
    return arrowVector;
  }

  private FieldVector writeString(final FieldVector arrowVector, final ColumnVector hiveVector,
      final int size, final VectorizedRowBatch batch,
      final IntAndVectorsConsumer nullSetter, final IntIntAndVectorsConsumer valueSetter,
      TypeInfo typeInfo) {

    if (encode) {
      final BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
      final VarCharVector varCharVector = (VarCharVector)
          Types.MinorType.VARCHAR.getNewVector(null,
              FieldType.nullable(Types.MinorType.VARCHAR.getType()), allocator, null);

      writeGeneric(varCharVector, hiveVector, size, batch, nullSetter, valueSetter, typeInfo);

      int j = 0;
      final Set<ByteBuffer> occurrences = new HashSet<>();
      for (int i = 0; i < size; i++) {
        if (!bytesVector.isNull[i]) {
          final ByteBuffer byteBuffer =
              ByteBuffer.wrap(bytesVector.vector[j], bytesVector.start[j],
                  bytesVector.length[j]);
          if (!occurrences.contains(byteBuffer)) {
            occurrences.add(byteBuffer);
          }
          j++;
        }
      }
      final FieldType fieldType = arrowVector.getField().getFieldType();
      final VarCharVector dictionaryVector = (VarCharVector) Types.MinorType.VARCHAR.
          getNewVector(null, fieldType, allocator, null);
      j = 0;
      for (ByteBuffer occurrence : occurrences) {
        final int start = occurrence.position();
        final int length = occurrence.limit() - start;
        dictionaryVector.setSafe(j++, occurrence.array(), start, length);
      }
      dictionaryVector.setValueCount(occurrences.size());
      varCharVector.setValueCount(size);
      final DictionaryEncoding dictionaryEncoding = arrowVector.getField().getDictionary();
      final Dictionary dictionary = new Dictionary(dictionaryVector, dictionaryEncoding);
      dictionaryProvider.put(dictionary);
      final IntVector encodedVector = (IntVector) DictionaryEncoder.encode(varCharVector,
          dictionary);
      encodedVector.makeTransferPair(arrowVector).transfer();
      return arrowVector;
    } else {
      return writeGeneric(arrowVector, hiveVector, size, batch, nullSetter, valueSetter, typeInfo);
    }
  }

  //nullSetters and valueSetter for each type

  //bool
  private static final IntAndVectorsConsumer boolNullSetter = (i, arrowVector, hiveVector)
      -> ((BitVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer boolValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((BitVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //byte
  private static final IntAndVectorsConsumer byteNullSetter = (i, arrowVector, hiveVector)
      -> ((TinyIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer byteValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((TinyIntVector) arrowVector).set(i, (byte) ((LongColumnVector) hiveVector).vector[j]);

  //short
  private static final IntAndVectorsConsumer shortNullSetter = (i, arrowVector, hiveVector)
      -> ((SmallIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer shortValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((SmallIntVector) arrowVector).set(i, (short) ((LongColumnVector) hiveVector).vector[j]);

  //int
  private static final IntAndVectorsConsumer intNullSetter = (i, arrowVector, hiveVector)
      -> ((IntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer intValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((IntVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //long
  private static final IntAndVectorsConsumer longNullSetter = (i, arrowVector, hiveVector)
      -> ((BigIntVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer longValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((BigIntVector) arrowVector).set(i, ((LongColumnVector) hiveVector).vector[j]);

  //float
  private static final IntAndVectorsConsumer floatNullSetter = (i, arrowVector, hiveVector)
      -> ((Float4Vector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer floatValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((Float4Vector) arrowVector).set(i, (float) ((DoubleColumnVector) hiveVector).vector[j]);

  //double
  private static final IntAndVectorsConsumer doubleNullSetter = (i, arrowVector, hiveVector)
      -> ((Float8Vector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer doubleValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((Float8Vector) arrowVector).set(i, ((DoubleColumnVector) hiveVector).vector[j]);

  //string/varchar
  private static final IntAndVectorsConsumer stringNullSetter = (i, arrowVector, hiveVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer stringValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarCharVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //fixed-length CHAR
  private static final IntAndVectorsConsumer charNullSetter = (i, arrowVector, hiveVector)
      -> ((VarCharVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer charValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
  private static final IntAndVectorsConsumer dateNullSetter = (i, arrowVector, hiveVector)
      -> ((DateDayVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer dateValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((DateDayVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //timestamp
  private static final IntAndVectorsConsumer timestampNullSetter = (i, arrowVector, hiveVector)
      -> ((TimeStampMicroTZVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer timestampValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
  private static final IntIntAndVectorsConsumer binaryValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
    ((VarBinaryVector) arrowVector).setSafe(i, bytesVector.vector[j], bytesVector.start[j], bytesVector.length[j]);
  };

  //decimal and decimal64
  private static final IntAndVectorsConsumer decimalNullSetter = (i, arrowVector, hiveVector)
      -> ((DecimalVector) arrowVector).setNull(i);
  private static final IntIntAndVectorsConsumer decimalValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> {
    final DecimalVector decimalVector = (DecimalVector) arrowVector;
    final int scale = decimalVector.getScale();
    decimalVector.set(i, ((DecimalColumnVector) hiveVector).vector[j].getHiveDecimal().bigDecimalValue().setScale(scale));
  };
  private static final IntIntAndVectorsConsumer decimal64ValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
  private static IntIntAndVectorsConsumer intervalYearMonthValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
      -> ((IntervalYearVector) arrowVector).set(i, (int) ((LongColumnVector) hiveVector).vector[j]);

  //interval day
  private static final IntAndVectorsConsumer intervalDayTimeNullSetter = (i, arrowVector, hiveVector)
      -> ((IntervalDayVector) arrowVector).setNull(i);
  private static IntIntAndVectorsConsumer intervalDayTimeValueSetter = (i, j, arrowVector, hiveVector, typeInfo)
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
    void accept(int i, int j, FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo);
  }

}
