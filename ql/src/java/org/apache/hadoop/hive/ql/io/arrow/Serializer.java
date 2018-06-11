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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARROW_BATCH_SIZE;
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

  private final Configuration conf;
  private final BufferAllocator bufferAllocator;
  private final boolean encode;
  private long dictionaryId;
  private DictionaryProvider.MapDictionaryProvider dictionaryProvider;

  Serializer(ArrowColumnarBatchSerDe serDe) throws SerDeException {
    MAX_BUFFERED_ROWS = HiveConf.getIntVar(serDe.conf, HIVE_ARROW_BATCH_SIZE);
    ArrowColumnarBatchSerDe.LOG.info("ArrowColumnarBatchSerDe max number of buffered columns: " +
        MAX_BUFFERED_ROWS);
    conf = serDe.conf;
    bufferAllocator = serDe.rootAllocator;
    encode = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ARROW_ENCODE);

    // Schema
    structTypeInfo = (StructTypeInfo) getTypeInfoFromObjectInspector(serDe.rowObjectInspector);
    List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    fieldSize = fieldTypeInfos.size();

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
    dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
    final List<Field> fieldList = new ArrayList<>();
    final List<FieldVector> vectorList = new ArrayList<>();

    for (int fieldIndex = 0; fieldIndex < vectorizedRowBatch.projectionSize; fieldIndex++) {
      final int projectedColumn = vectorizedRowBatch.projectedColumns[fieldIndex];
      final ColumnVector hiveVector = vectorizedRowBatch.cols[projectedColumn];
      final TypeInfo fieldTypeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(fieldIndex);
      final String fieldName = structTypeInfo.getAllStructFieldNames().get(fieldIndex);
      final Field field = toField(fieldName, fieldTypeInfo);
      final FieldVector arrowVector = field.createVector(bufferAllocator);
      arrowVector.setInitialCapacity(batchSize);
      arrowVector.allocateNew();
      vectorList.add(write(arrowVector, hiveVector, fieldTypeInfo, batchSize, encode));
      fieldList.add(arrowVector.getField());
      arrowVector.setValueCount(batchSize);
    }
    vectorizedRowBatch.reset();

    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fieldList, vectorList, batchSize);
    batchSize = 0;
    return new ArrowWrapperWritable(vectorSchemaRoot, dictionaryProvider);
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
            // HIVE-19723: Prefer microsecond because Spark supports it
            return Types.MinorType.TIMESTAMPMICRO.getType();
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

  @SuppressWarnings("unchecked")
  private FieldVector write(FieldVector arrowVector, ColumnVector hiveVector, TypeInfo typeInfo,
      int size, boolean encode) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return writePrimitive(arrowVector, hiveVector, (PrimitiveTypeInfo) typeInfo, size, encode);
      case LIST:
        return writeList((ListVector) arrowVector, (ListColumnVector) hiveVector,
            (ListTypeInfo) typeInfo, size, encode);
      case STRUCT:
        return writeStruct((MapVector) arrowVector, (StructColumnVector) hiveVector,
            (StructTypeInfo) typeInfo, size, encode);
      case UNION:
        return writeUnion(arrowVector, hiveVector, typeInfo, size, encode);
      case MAP:
        return writeMap((ListVector) arrowVector, (MapColumnVector) hiveVector,
            (MapTypeInfo) typeInfo, size, encode);
      default:
        throw new IllegalArgumentException();
    }
  }

  private FieldVector writeMap(ListVector arrowVector, MapColumnVector hiveVector,
      MapTypeInfo typeInfo, int size, boolean encode) {
    final ListTypeInfo structListTypeInfo = toStructListTypeInfo(typeInfo);
    final ListColumnVector structListVector = toStructListVector(hiveVector);

    write(arrowVector, structListVector, structListTypeInfo, size, encode);

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
      TypeInfo typeInfo, int size, boolean encode) {
    final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
    final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
    final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
    final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;

    final int tag = hiveUnionVector.tags[0];
    final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
    final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);

    write(arrowVector, hiveObjectVector, objectTypeInfo, size, encode);

    return arrowVector;
  }

  @SuppressWarnings("unchecked")
  private FieldVector writeStruct(MapVector arrowVector, StructColumnVector hiveVector,
      StructTypeInfo typeInfo, int size, boolean encode) {
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
      write(arrowFieldVector, hiveFieldVector, fieldTypeInfo, size, encode);
    }

    final ArrowBuf validityBuffer = arrowVector.getValidityBuffer();
    for (int rowIndex = 0; rowIndex < size; rowIndex++) {
      if (hiveVector.isNull[rowIndex]) {
        BitVectorHelper.setValidityBit(validityBuffer, rowIndex, 0);
      } else {
        BitVectorHelper.setValidityBitToOne(validityBuffer, rowIndex);
      }
    }

    return (FieldVector) arrowVector;
  }

  private FieldVector writeList(ListVector arrowVector, ListColumnVector hiveVector,
      ListTypeInfo typeInfo, int size, boolean encode) {
    final int OFFSET_WIDTH = 4;
    final TypeInfo elementTypeInfo = typeInfo.getListElementTypeInfo();
    final ColumnVector hiveElementVector = hiveVector.child;
    final FieldVector arrowElementVector =
        (FieldVector) arrowVector.addOrGetVector(toFieldType(elementTypeInfo)).getVector();
    arrowElementVector.setInitialCapacity(hiveVector.childCount);
    arrowElementVector.allocateNew();

    write(arrowElementVector, hiveElementVector, elementTypeInfo, hiveVector.childCount, encode);

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
    return arrowVector;
  }

  private FieldVector writePrimitive(FieldVector arrowVector, ColumnVector hiveVector,
      PrimitiveTypeInfo primitiveTypeInfo, int size, boolean encode) {
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
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
          return bitVector;
        }
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
          return tinyIntVector;
        }
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
          return smallIntVector;
        }
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
          return intVector;
        }
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
          return bigIntVector;
        }
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
          return float4Vector;
        }
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
          return float8Vector;
        }
      case VARCHAR:
      case CHAR:
      case STRING:
        {
          if (encode) {
            final BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
            final VarCharVector varCharVector = (VarCharVector)
                Types.MinorType.VARCHAR.getNewVector(null,
                    FieldType.nullable(Types.MinorType.VARCHAR.getType()), bufferAllocator, null);
            for (int i = 0; i < size; i++) {
              if (hiveVector.isNull[i]) {
                varCharVector.setNull(i);
              } else {
                varCharVector.setSafe(i, bytesVector.vector[i], bytesVector.start[i],
                    bytesVector.length[i]);
              }
            }
            arrowVector.setValueCount(size);

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
                getNewVector(null, fieldType, bufferAllocator, null);
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
            final BytesColumnVector bytesVector = (BytesColumnVector) hiveVector;
            final VarCharVector varCharVector = (VarCharVector) arrowVector;
            for (int i = 0; i < size; i++) {
              if (hiveVector.isNull[i]) {
                varCharVector.setNull(i);
              } else {
                varCharVector.setSafe(i, bytesVector.vector[i], bytesVector.start[i],
                    bytesVector.length[i]);
              }
            }
            varCharVector.setValueCount(size);

            return varCharVector;
          }
        }
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
          return dateDayVector;
        }
      case TIMESTAMP:
        {
          final TimeStampMicroVector timeStampMicroVector = (TimeStampMicroVector) arrowVector;
          final TimestampColumnVector timestampColumnVector = (TimestampColumnVector) hiveVector;
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              timeStampMicroVector.setNull(i);
            } else {
              // Time = second + sub-second
              final long secondInMillis = timestampColumnVector.getTime(i);
              final long secondInMicros = (secondInMillis - secondInMillis % MILLIS_PER_SECOND) * MICROS_PER_MILLIS;
              final long subSecondInMicros = timestampColumnVector.getNanos(i) / NS_PER_MICROS;

              if ((secondInMillis > 0 && secondInMicros < 0) || (secondInMillis < 0 && secondInMicros > 0)) {
                // If the timestamp cannot be represented in long microsecond, set it as a null value
                timeStampMicroVector.setNull(i);
              } else {
                timeStampMicroVector.set(i, secondInMicros + subSecondInMicros);
              }
            }
          }
          return timeStampMicroVector;
        }
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
          return varBinaryVector;
        }
      case DECIMAL:
        {
          final DecimalVector decimalVector = (DecimalVector) arrowVector;
          final int scale = decimalVector.getScale();
          for (int i = 0; i < size; i++) {
            if (hiveVector.isNull[i]) {
              decimalVector.setNull(i);
            } else {
              decimalVector.set(i, ((DecimalColumnVector) hiveVector).vector[i].getHiveDecimal().
                  bigDecimalValue().setScale(scale));
            }
          }
          return decimalVector;
        }
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
          return intervalYearVector;
        }
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
          return intervalDayVector;
        }
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
