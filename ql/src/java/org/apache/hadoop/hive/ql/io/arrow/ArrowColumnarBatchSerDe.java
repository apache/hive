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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.BitWriter;
import org.apache.arrow.vector.complex.writer.DateDayWriter;
import org.apache.arrow.vector.complex.writer.DecimalWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.Float4Writer;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.IntervalDayWriter;
import org.apache.arrow.vector.complex.writer.IntervalYearWriter;
import org.apache.arrow.vector.complex.writer.SmallIntWriter;
import org.apache.arrow.vector.complex.writer.TimeStampMilliWriter;
import org.apache.arrow.vector.complex.writer.TinyIntWriter;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.IntConsumer;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARROW_BATCH_SIZE;
import static org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil.createColumnVector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromObjectInspector;

/**
 * ArrowColumnarBatchSerDe converts Apache Hive rows to Apache Arrow columns. Its serialized
 * class is {@link ArrowWrapperWritable}, which doesn't support {@link
 * Writable#readFields(DataInput)} and {@link Writable#write(DataOutput)}.
 *
 * Followings are known issues of current implementation.
 *
 * A list column cannot have a decimal column. {@link UnionListWriter} doesn't have an
 * implementation for {@link BaseWriter.ListWriter#decimal()}.
 *
 * A union column can have only one of string, char, varchar fields at a same time. Apache Arrow
 * doesn't have string and char, so {@link ArrowColumnarBatchSerDe} uses varchar to simulate
 * string and char. They will be considered as a same data type in
 * {@link org.apache.arrow.vector.complex.UnionVector}.
 *
 * Timestamp with local timezone is not supported. {@link VectorAssignRow} doesn't support it.
 */
public class ArrowColumnarBatchSerDe extends AbstractSerDe {
  public static final Logger LOG = LoggerFactory.getLogger(ArrowColumnarBatchSerDe.class.getName());
  private static final String DEFAULT_ARROW_FIELD_NAME = "[DEFAULT]";

  private static final int MS_PER_SECOND = 1_000;
  private static final int MS_PER_MINUTE = MS_PER_SECOND * 60;
  private static final int MS_PER_HOUR = MS_PER_MINUTE * 60;
  private static final int MS_PER_DAY = MS_PER_HOUR * 24;
  private static final int NS_PER_MS = 1_000_000;

  private BufferAllocator rootAllocator;

  private StructTypeInfo rowTypeInfo;
  private StructObjectInspector rowObjectInspector;
  private Configuration conf;
  private Serializer serializer;
  private Deserializer deserializer;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    this.conf = conf;

    rootAllocator = RootAllocatorFactory.INSTANCE.getRootAllocator(conf);

    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);

    // Create an object inspector
    final List<String> columnNames;
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    final List<TypeInfo> columnTypes;
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector =
        (StructObjectInspector) getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);

    final List<Field> fields = new ArrayList<>();
    final int size = columnNames.size();
    for (int i = 0; i < size; i++) {
      fields.add(toField(columnNames.get(i), columnTypes.get(i)));
    }

    serializer = new Serializer(new Schema(fields));
    deserializer = new Deserializer();
  }

  private class Serializer {
    private final int MAX_BUFFERED_ROWS;

    // Schema
    private final StructTypeInfo structTypeInfo;
    private final List<TypeInfo> fieldTypeInfos;
    private final int fieldSize;

    // Hive columns
    private final VectorizedRowBatch vectorizedRowBatch;
    private final VectorAssignRow vectorAssignRow;
    private int batchSize;

    // Arrow columns
    private final VectorSchemaRoot vectorSchemaRoot;
    private final List<FieldVector> arrowVectors;
    private final List<FieldWriter> fieldWriters;

    private Serializer(Schema schema) throws SerDeException {
      MAX_BUFFERED_ROWS = HiveConf.getIntVar(conf, HIVE_ARROW_BATCH_SIZE);
      LOG.info("ArrowColumnarBatchSerDe max number of buffered columns: " + MAX_BUFFERED_ROWS);

      // Schema
      structTypeInfo = (StructTypeInfo) getTypeInfoFromObjectInspector(rowObjectInspector);
      fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      fieldSize = fieldTypeInfos.size();

      // Init Arrow stuffs
      vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
      arrowVectors = vectorSchemaRoot.getFieldVectors();
      fieldWriters = Lists.newArrayList();
      for (FieldVector fieldVector : arrowVectors) {
        final FieldWriter fieldWriter =
            Types.getMinorTypeForArrowType(
                fieldVector.getField().getType()).getNewFieldWriter(fieldVector);
        fieldWriters.add(fieldWriter);
      }

      // Init Hive stuffs
      vectorizedRowBatch = new VectorizedRowBatch(fieldSize);
      for (int i = 0; i < fieldSize; i++) {
        final ColumnVector columnVector = createColumnVector(fieldTypeInfos.get(i));
        vectorizedRowBatch.cols[i] = columnVector;
        columnVector.init();
      }
      vectorizedRowBatch.ensureSize(MAX_BUFFERED_ROWS);
      vectorAssignRow = new VectorAssignRow();
      try {
        vectorAssignRow.init(rowObjectInspector);
      } catch (HiveException e) {
        throw new SerDeException(e);
      }
    }

    private ArrowWrapperWritable serializeBatch() {
      for (int i = 0; i < vectorizedRowBatch.projectionSize; i++) {
        final int projectedColumn = vectorizedRowBatch.projectedColumns[i];
        final ColumnVector hiveVector = vectorizedRowBatch.cols[projectedColumn];
        final TypeInfo fieldTypeInfo = structTypeInfo.getAllStructFieldTypeInfos().get(i);
        final FieldWriter fieldWriter = fieldWriters.get(i);
        final FieldVector arrowVector = arrowVectors.get(i);
        arrowVector.setValueCount(0);
        fieldWriter.setPosition(0);
        write(fieldWriter, arrowVector, hiveVector, fieldTypeInfo, 0, batchSize, true);
      }
      vectorizedRowBatch.reset();
      vectorSchemaRoot.setRowCount(batchSize);

      batchSize = 0;
      return new ArrowWrapperWritable(vectorSchemaRoot);
    }

    private BaseWriter getWriter(FieldWriter writer, TypeInfo typeInfo, String name) {
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
              return writer.bit(name);
            case BYTE:
              return writer.tinyInt(name);
            case SHORT:
              return writer.smallInt(name);
            case INT:
              return writer.integer(name);
            case LONG:
              return writer.bigInt(name);
            case FLOAT:
              return writer.float4(name);
            case DOUBLE:
              return writer.float8(name);
            case STRING:
            case VARCHAR:
            case CHAR:
              return writer.varChar(name);
            case DATE:
              return writer.dateDay(name);
            case TIMESTAMP:
              return writer.timeStampMilli(name);
            case BINARY:
              return writer.varBinary(name);
            case DECIMAL:
              final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
              final int scale = decimalTypeInfo.scale();
              final int precision = decimalTypeInfo.precision();
              return writer.decimal(name, scale, precision);
            case INTERVAL_YEAR_MONTH:
              return writer.intervalYear(name);
            case INTERVAL_DAY_TIME:
              return writer.intervalDay(name);
            case TIMESTAMPLOCALTZ: // VectorAssignRow doesn't support it
            case VOID:
            case UNKNOWN:
            default:
              throw new IllegalArgumentException();
          }
        case LIST:
        case UNION:
          return writer.list(name);
        case STRUCT:
          return writer.map(name);
        case MAP: // The caller will convert map to array<struct>
          return writer.list(name).map();
        default:
          throw new IllegalArgumentException();
      }
    }

    private BaseWriter getWriter(FieldWriter writer, TypeInfo typeInfo) {
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
              return writer.bit();
            case BYTE:
              return writer.tinyInt();
            case SHORT:
              return writer.smallInt();
            case INT:
              return writer.integer();
            case LONG:
              return writer.bigInt();
            case FLOAT:
              return writer.float4();
            case DOUBLE:
              return writer.float8();
            case STRING:
            case VARCHAR:
            case CHAR:
              return writer.varChar();
            case DATE:
              return writer.dateDay();
            case TIMESTAMP:
              return writer.timeStampMilli();
            case BINARY:
              return writer.varBinary();
            case INTERVAL_YEAR_MONTH:
              return writer.intervalDay();
            case INTERVAL_DAY_TIME:
              return writer.intervalYear();
            case TIMESTAMPLOCALTZ: // VectorAssignRow doesn't support it
            case DECIMAL: // ListVector doesn't support it
            case VOID:
            case UNKNOWN:
            default:
              throw new IllegalArgumentException();
          }
        case LIST:
        case UNION:
          return writer.list();
        case STRUCT:
          return writer.map();
        case MAP: // The caller will convert map to array<struct>
          return writer.list().map();
        default:
          throw new IllegalArgumentException();
      }
    }

    private void write(BaseWriter baseWriter, FieldVector arrowVector, ColumnVector hiveVector,
        TypeInfo typeInfo, int offset, int length, boolean incrementIndex) {

      final IntConsumer writer;
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
          switch (primitiveCategory) {
            case BOOLEAN:
              writer = index -> ((BitWriter) baseWriter).writeBit(
                  (int) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case BYTE:
              writer = index ->
                  ((TinyIntWriter) baseWriter).writeTinyInt(
                      (byte) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case SHORT:
              writer = index -> ((SmallIntWriter) baseWriter).writeSmallInt(
                  (short) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case INT:
              writer = index -> ((IntWriter) baseWriter).writeInt(
                  (int) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case LONG:
              writer = index -> ((BigIntWriter) baseWriter).writeBigInt(
                  ((LongColumnVector) hiveVector).vector[index]);
              break;
            case FLOAT:
              writer = index -> ((Float4Writer) baseWriter).writeFloat4(
                  (float) ((DoubleColumnVector) hiveVector).vector[index]);
              break;
            case DOUBLE:
              writer = index -> ((Float8Writer) baseWriter).writeFloat8(
                  ((DoubleColumnVector) hiveVector).vector[index]);
              break;
            case STRING:
            case VARCHAR:
            case CHAR:
              writer = index -> {
                BytesColumnVector stringVector = (BytesColumnVector) hiveVector;
                byte[] bytes = stringVector.vector[index];
                int start = stringVector.start[index];
                int bytesLength = stringVector.length[index];
                try (ArrowBuf arrowBuf = rootAllocator.buffer(bytesLength)) {
                  arrowBuf.setBytes(0, bytes, start, bytesLength);
                  ((VarCharWriter) baseWriter).writeVarChar(0, bytesLength, arrowBuf);
                }
              };
              break;
            case DATE:
              writer = index -> ((DateDayWriter) baseWriter).writeDateDay(
                  (int) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case TIMESTAMP:
              writer = index -> ((TimeStampMilliWriter) baseWriter).writeTimeStampMilli(
                  ((TimestampColumnVector) hiveVector).getTime(index));
              break;
            case BINARY:
              writer = index -> {
                BytesColumnVector binaryVector = (BytesColumnVector) hiveVector;
                final byte[] bytes = binaryVector.vector[index];
                final int start = binaryVector.start[index];
                final int byteLength = binaryVector.length[index];
                try (ArrowBuf arrowBuf = rootAllocator.buffer(byteLength)) {
                  arrowBuf.setBytes(0, bytes, start, byteLength);
                  ((VarBinaryWriter) baseWriter).writeVarBinary(0, byteLength, arrowBuf);
                }
              };
              break;
            case DECIMAL:
              writer = index -> {
                DecimalColumnVector hiveDecimalVector = (DecimalColumnVector) hiveVector;
                ((DecimalWriter) baseWriter).writeDecimal(
                    hiveDecimalVector.vector[index].getHiveDecimal().bigDecimalValue()
                        .setScale(hiveDecimalVector.scale));
              };
              break;
            case INTERVAL_YEAR_MONTH:
              writer = index -> ((IntervalYearWriter) baseWriter).writeIntervalYear(
                  (int) ((LongColumnVector) hiveVector).vector[index]);
              break;
            case INTERVAL_DAY_TIME:
              writer = index -> {
                IntervalDayTimeColumnVector intervalDayTimeVector =
                    (IntervalDayTimeColumnVector) hiveVector;
                final long millis = (intervalDayTimeVector.getTotalSeconds(index) * 1_000) +
                    (intervalDayTimeVector.getNanos(index) / 1_000_000);
                final int days = (int) (millis / MS_PER_DAY);
                ((IntervalDayWriter) baseWriter).writeIntervalDay(
                    days, (int) (millis % MS_PER_DAY));
              };
              break;
            case VOID:
            case UNKNOWN:
            case TIMESTAMPLOCALTZ:
            default:
              throw new IllegalArgumentException();
          }
          break;
        case LIST:
          final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
          final TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
          final ListColumnVector hiveListVector = (ListColumnVector) hiveVector;
          final ColumnVector hiveElementVector = hiveListVector.child;
          final FieldVector arrowElementVector = arrowVector.getChildrenFromFields().get(0);
          final BaseWriter.ListWriter listWriter = (BaseWriter.ListWriter) baseWriter;
          final BaseWriter elementWriter = getWriter((FieldWriter) baseWriter, elementTypeInfo);

          writer = index -> {
            final int listOffset = (int) hiveListVector.offsets[index];
            final int listLength = (int) hiveListVector.lengths[index];
            listWriter.startList();
            write(elementWriter, arrowElementVector, hiveElementVector, elementTypeInfo,
                listOffset, listLength, false);
            listWriter.endList();
          };

          incrementIndex = false;
          break;
        case STRUCT:
          final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
          final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
          final StructColumnVector hiveStructVector = (StructColumnVector) hiveVector;
          final List<FieldVector> arrowFieldVectors = arrowVector.getChildrenFromFields();
          final ColumnVector[] hiveFieldVectors = hiveStructVector.fields;
          final BaseWriter.MapWriter structWriter = (BaseWriter.MapWriter) baseWriter;
          final int fieldSize = fieldTypeInfos.size();

          writer = index -> {
            structWriter.start();
            for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
              final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
              final String fieldName = structTypeInfo.getAllStructFieldNames().get(fieldIndex);
              final ColumnVector hiveFieldVector = hiveFieldVectors[fieldIndex];
              final BaseWriter fieldWriter = getWriter((FieldWriter) structWriter, fieldTypeInfo,
                  fieldName);
              final FieldVector arrowFieldVector = arrowFieldVectors.get(fieldIndex);
              write(fieldWriter, arrowFieldVector, hiveFieldVector, fieldTypeInfo, index, 1, false);
            }
            structWriter.end();
          };

          incrementIndex = false;
          break;
        case UNION:
          final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
          final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
          final UnionColumnVector hiveUnionVector = (UnionColumnVector) hiveVector;
          final ColumnVector[] hiveObjectVectors = hiveUnionVector.fields;
          final UnionWriter unionWriter = (UnionWriter) baseWriter;

          writer = index -> {
            final int tag = hiveUnionVector.tags[index];
            final ColumnVector hiveObjectVector = hiveObjectVectors[tag];
            final TypeInfo objectTypeInfo = objectTypeInfos.get(tag);
            write(unionWriter, arrowVector, hiveObjectVector, objectTypeInfo, index, 1, false);
          };
          break;
        case MAP:
          final ListTypeInfo structListTypeInfo =
              toStructListTypeInfo((MapTypeInfo) typeInfo);
          final ListColumnVector structListVector =
              toStructListVector((MapColumnVector) hiveVector);

          writer = index -> write(baseWriter, arrowVector, structListVector, structListTypeInfo,
              index, length, false);

          incrementIndex = false;
          break;
        default:
          throw new IllegalArgumentException();
      }

      if (hiveVector.noNulls) {
        if (hiveVector.isRepeating) {
          for (int i = 0; i < length; i++) {
            writer.accept(0);
            if (incrementIndex) {
              baseWriter.setPosition(baseWriter.getPosition() + 1);
            }
          }
        } else {
          if (vectorizedRowBatch.selectedInUse) {
            for (int j = 0; j < length; j++) {
              final int i = vectorizedRowBatch.selected[j];
              writer.accept(offset + i);
              if (incrementIndex) {
                baseWriter.setPosition(baseWriter.getPosition() + 1);
              }
            }
          } else {
            for (int i = 0; i < length; i++) {
              writer.accept(offset + i);
              if (incrementIndex) {
                baseWriter.setPosition(baseWriter.getPosition() + 1);
              }
            }
          }
        }
      } else {
        if (hiveVector.isRepeating) {
          for (int i = 0; i < length; i++) {
            if (hiveVector.isNull[0]) {
              writeNull(baseWriter);
            } else {
              writer.accept(0);
            }
            if (incrementIndex) {
              baseWriter.setPosition(baseWriter.getPosition() + 1);
            }
          }
        } else {
          if (vectorizedRowBatch.selectedInUse) {
            for (int j = 0; j < length; j++) {
              final int i = vectorizedRowBatch.selected[j];
              if (hiveVector.isNull[offset + i]) {
                writeNull(baseWriter);
              } else {
                writer.accept(offset + i);
              }
              if (incrementIndex) {
                baseWriter.setPosition(baseWriter.getPosition() + 1);
              }
            }
          } else {
            for (int i = 0; i < length; i++) {
              if (hiveVector.isNull[offset + i]) {
                writeNull(baseWriter);
              } else {
                writer.accept(offset + i);
              }
              if (incrementIndex) {
                baseWriter.setPosition(baseWriter.getPosition() + 1);
              }
            }
          }
        }
      }
    }

    public ArrowWrapperWritable serialize(Object obj, ObjectInspector objInspector) {
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

  private static void writeNull(BaseWriter baseWriter) {
    if (baseWriter instanceof UnionListWriter) {
      // UnionListWriter should implement AbstractFieldWriter#writeNull
      BaseWriter.ListWriter listWriter = ((UnionListWriter) baseWriter).list();
      listWriter.setPosition(listWriter.getPosition() + 1);
    } else {
      // FieldWriter should have a super method of AbstractFieldWriter#writeNull
      try {
        Method method = baseWriter.getClass().getMethod("writeNull");
        method.setAccessible(true);
        method.invoke(baseWriter);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static abstract class PrimitiveReader {
    final void read(FieldReader reader, ColumnVector columnVector, int offset, int length) {
      for (int i = 0; i < length; i++) {
        final int rowIndex = offset + i;
        if (reader.isSet()) {
          doRead(reader, columnVector, rowIndex);
        } else {
          VectorizedBatchUtil.setNullColIsNullValue(columnVector, rowIndex);
        }
        reader.setPosition(reader.getPosition() + 1);
      }
    }

    abstract void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex);
  }

  private class Deserializer {
    private final VectorExtractRow vectorExtractRow;
    private final VectorizedRowBatch vectorizedRowBatch;
    private Object[][] rows;

    public Deserializer() throws SerDeException {
      vectorExtractRow = new VectorExtractRow();
      final List<TypeInfo> fieldTypeInfoList = rowTypeInfo.getAllStructFieldTypeInfos();
      final int fieldCount = fieldTypeInfoList.size();
      final TypeInfo[] typeInfos = fieldTypeInfoList.toArray(new TypeInfo[fieldCount]);
      try {
        vectorExtractRow.init(typeInfos);
      } catch (HiveException e) {
        throw new SerDeException(e);
      }

      vectorizedRowBatch = new VectorizedRowBatch(fieldCount);
      for (int i = 0; i < fieldCount; i++) {
        final ColumnVector columnVector = createColumnVector(typeInfos[i]);
        columnVector.init();
        vectorizedRowBatch.cols[i] = columnVector;
      }
    }

    public Object deserialize(Writable writable) {
      final ArrowWrapperWritable arrowWrapperWritable = (ArrowWrapperWritable) writable;
      final VectorSchemaRoot vectorSchemaRoot = arrowWrapperWritable.getVectorSchemaRoot();
      final List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      final int fieldCount = fieldVectors.size();
      final int rowCount = vectorSchemaRoot.getRowCount();
      vectorizedRowBatch.ensureSize(rowCount);

      if (rows == null || rows.length < rowCount ) {
        rows = new Object[rowCount][];
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
          rows[rowIndex] = new Object[fieldCount];
        }
      }

      for (int i = 0; i < fieldCount; i++) {
        final FieldVector fieldVector = fieldVectors.get(i);
        final FieldReader fieldReader = fieldVector.getReader();
        fieldReader.setPosition(0);
        final int projectedCol = vectorizedRowBatch.projectedColumns[i];
        final ColumnVector columnVector = vectorizedRowBatch.cols[projectedCol];
        final TypeInfo typeInfo = rowTypeInfo.getAllStructFieldTypeInfos().get(i);
        read(fieldReader, columnVector, typeInfo, 0, rowCount);
      }
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        vectorExtractRow.extractRow(vectorizedRowBatch, rowIndex, rows[rowIndex]);
      }
      vectorizedRowBatch.reset();
      return rows;
    }

    private void read(FieldReader reader, ColumnVector columnVector, TypeInfo typeInfo,
        int rowOffset, int rowLength) {
      switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
              ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
          final PrimitiveReader primitiveReader;
          switch (primitiveCategory) {
            case BOOLEAN:
              primitiveReader = new PrimitiveReader() {
                NullableBitHolder holder = new NullableBitHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case BYTE:
              primitiveReader = new PrimitiveReader() {
                NullableTinyIntHolder holder = new NullableTinyIntHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case SHORT:
              primitiveReader = new PrimitiveReader() {
                NullableSmallIntHolder holder = new NullableSmallIntHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case INT:
              primitiveReader = new PrimitiveReader() {
                NullableIntHolder holder = new NullableIntHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case LONG:
              primitiveReader = new PrimitiveReader() {
                NullableBigIntHolder holder = new NullableBigIntHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case FLOAT:
              primitiveReader = new PrimitiveReader() {
                NullableFloat4Holder holder = new NullableFloat4Holder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((DoubleColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case DOUBLE:
              primitiveReader = new PrimitiveReader() {
                NullableFloat8Holder holder = new NullableFloat8Holder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((DoubleColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case STRING:
            case VARCHAR:
            case CHAR:
              primitiveReader = new PrimitiveReader() {
                NullableVarCharHolder holder = new NullableVarCharHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  int varCharSize = holder.end - holder.start;
                  byte[] varCharBytes = new byte[varCharSize];
                  holder.buffer.getBytes(holder.start, varCharBytes);
                  ((BytesColumnVector) columnVector).setVal(rowIndex, varCharBytes, 0, varCharSize);
                }
              };
              break;
            case DATE:
              primitiveReader = new PrimitiveReader() {
                NullableDateDayHolder holder = new NullableDateDayHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case TIMESTAMP:
              primitiveReader = new PrimitiveReader() {
                NullableTimeStampMilliHolder timeStampMilliHolder =
                    new NullableTimeStampMilliHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(timeStampMilliHolder);
                  ((TimestampColumnVector) columnVector).set(rowIndex,
                      new Timestamp(timeStampMilliHolder.value));
                }
              };
              break;
            case BINARY:
              primitiveReader = new PrimitiveReader() {
                NullableVarBinaryHolder holder = new NullableVarBinaryHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  final int binarySize = holder.end - holder.start;
                  final byte[] binaryBytes = new byte[binarySize];
                  holder.buffer.getBytes(holder.start, binaryBytes);
                  ((BytesColumnVector) columnVector).setVal(rowIndex, binaryBytes, 0, binarySize);
                }
              };
              break;
            case DECIMAL:
              primitiveReader = new PrimitiveReader() {
                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  ((DecimalColumnVector) columnVector).set(rowIndex,
                      HiveDecimal.create(reader.readBigDecimal()));
                }
              };
              break;
            case INTERVAL_YEAR_MONTH:
              primitiveReader = new PrimitiveReader() {
                NullableIntervalYearHolder holder = new NullableIntervalYearHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  reader.read(holder);
                  ((LongColumnVector) columnVector).vector[rowIndex] = holder.value;
                }
              };
              break;
            case INTERVAL_DAY_TIME:
              primitiveReader = new PrimitiveReader() {
                NullableIntervalDayHolder holder = new NullableIntervalDayHolder();

                @Override
                void doRead(FieldReader reader, ColumnVector columnVector, int rowIndex) {
                  IntervalDayTimeColumnVector intervalDayTimeVector =
                      (IntervalDayTimeColumnVector) columnVector;
                  reader.read(holder);
                  HiveIntervalDayTime intervalDayTime = new HiveIntervalDayTime(
                      holder.days, // days
                      holder.milliseconds / MS_PER_HOUR, // hour
                      (holder.milliseconds % MS_PER_HOUR) / MS_PER_MINUTE, // minute
                      (holder.milliseconds % MS_PER_MINUTE) / MS_PER_SECOND, // second
                      (holder.milliseconds % MS_PER_SECOND) * NS_PER_MS); // nanosecond
                  intervalDayTimeVector.set(rowIndex, intervalDayTime);
                }
              };
              break;
            default:
              throw new IllegalArgumentException();
          }
          primitiveReader.read(reader, columnVector, rowOffset, rowLength);
          break;
        case LIST:
          final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
          final TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
          final ListColumnVector listVector = (ListColumnVector) columnVector;
          final ColumnVector elementVector = listVector.child;
          final FieldReader elementReader = reader.reader();

          int listOffset = 0;
          for (int rowIndex = 0; rowIndex < rowLength; rowIndex++) {
            final int adjustedRowIndex = rowOffset + rowIndex;
            reader.setPosition(adjustedRowIndex);
            final int listLength = reader.size();
            listVector.offsets[adjustedRowIndex] = listOffset;
            listVector.lengths[adjustedRowIndex] = listLength;
            read(elementReader, elementVector, elementTypeInfo, listOffset, listLength);
            listOffset += listLength;
          }
          break;
        case STRUCT:
          final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
          final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
          final List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
          final int fieldSize = fieldNames.size();
          final StructColumnVector structVector = (StructColumnVector) columnVector;
          final ColumnVector[] fieldVectors = structVector.fields;

          for (int fieldIndex = 0; fieldIndex < fieldSize; fieldIndex++) {
            final TypeInfo fieldTypeInfo = fieldTypeInfos.get(fieldIndex);
            final FieldReader fieldReader = reader.reader(fieldNames.get(fieldIndex));
            final ColumnVector fieldVector = fieldVectors[fieldIndex];
            read(fieldReader, fieldVector, fieldTypeInfo, rowOffset, rowLength);
          }
          break;
        case UNION:
          final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
          final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
          final UnionColumnVector unionVector = (UnionColumnVector) columnVector;
          final ColumnVector[] objectVectors = unionVector.fields;
          final Map<Types.MinorType, Integer> minorTypeToTagMap = Maps.newHashMap();
          for (int tag = 0; tag < objectTypeInfos.size(); tag++) {
            minorTypeToTagMap.put(toMinorType(objectTypeInfos.get(tag)), tag);
          }

          final UnionReader unionReader = (UnionReader) reader;
          for (int rowIndex = 0; rowIndex < rowLength; rowIndex++) {
            final int adjustedRowIndex = rowIndex + rowOffset;
            unionReader.setPosition(adjustedRowIndex);
            final Types.MinorType minorType = unionReader.getMinorType();
            final int tag = minorTypeToTagMap.get(minorType);
            unionVector.tags[adjustedRowIndex] = tag;
            read(unionReader, objectVectors[tag], objectTypeInfos.get(tag), adjustedRowIndex, 1);
          }
          break;
        case MAP:
          final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
          final ListTypeInfo mapStructListTypeInfo = toStructListTypeInfo(mapTypeInfo);
          final MapColumnVector hiveMapVector = (MapColumnVector) columnVector;
          final ListColumnVector mapStructListVector = toStructListVector(hiveMapVector);
          final StructColumnVector mapStructVector = (StructColumnVector) mapStructListVector.child;
          read(reader, mapStructListVector, mapStructListTypeInfo, rowOffset, rowLength);

          hiveMapVector.isRepeating = mapStructListVector.isRepeating;
          hiveMapVector.childCount = mapStructListVector.childCount;
          hiveMapVector.noNulls = mapStructListVector.noNulls;
          System.arraycopy(mapStructListVector.offsets, 0, hiveMapVector.offsets, 0, rowLength);
          System.arraycopy(mapStructListVector.lengths, 0, hiveMapVector.lengths, 0, rowLength);
          hiveMapVector.keys = mapStructVector.fields[0];
          hiveMapVector.values = mapStructVector.fields[1];
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  private static Types.MinorType toMinorType(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
          case BOOLEAN:
            return Types.MinorType.BIT;
          case BYTE:
            return Types.MinorType.TINYINT;
          case SHORT:
            return Types.MinorType.SMALLINT;
          case INT:
            return Types.MinorType.INT;
          case LONG:
            return Types.MinorType.BIGINT;
          case FLOAT:
            return Types.MinorType.FLOAT4;
          case DOUBLE:
            return Types.MinorType.FLOAT8;
          case STRING:
          case VARCHAR:
          case CHAR:
            return Types.MinorType.VARCHAR;
          case DATE:
            return Types.MinorType.DATEDAY;
          case TIMESTAMP:
            return Types.MinorType.TIMESTAMPMILLI;
          case BINARY:
            return Types.MinorType.VARBINARY;
          case DECIMAL:
            return Types.MinorType.DECIMAL;
          case INTERVAL_YEAR_MONTH:
            return Types.MinorType.INTERVALYEAR;
          case INTERVAL_DAY_TIME:
            return Types.MinorType.INTERVALDAY;
          case VOID:
          case TIMESTAMPLOCALTZ:
          case UNKNOWN:
          default:
            throw new IllegalArgumentException();
        }
      case LIST:
        return Types.MinorType.LIST;
      case STRUCT:
        return Types.MinorType.MAP;
      case UNION:
        return Types.MinorType.UNION;
      case MAP:
        // Apache Arrow doesn't have a map vector, so it's converted to a list vector of a struct
        // vector.
        return Types.MinorType.LIST;
      default:
        throw new IllegalArgumentException();
    }
  }

  private static ListTypeInfo toStructListTypeInfo(MapTypeInfo mapTypeInfo) {
    final StructTypeInfo structTypeInfo = new StructTypeInfo();
    structTypeInfo.setAllStructFieldNames(Lists.newArrayList("keys", "values"));
    structTypeInfo.setAllStructFieldTypeInfos(Lists.newArrayList(
        mapTypeInfo.getMapKeyTypeInfo(), mapTypeInfo.getMapValueTypeInfo()));
    final ListTypeInfo structListTypeInfo = new ListTypeInfo();
    structListTypeInfo.setListElementTypeInfo(structTypeInfo);
    return structListTypeInfo;
  }

  private static Field toField(String name, TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case BOOLEAN:
            return Field.nullable(name, Types.MinorType.BIT.getType());
          case BYTE:
            return Field.nullable(name, Types.MinorType.TINYINT.getType());
          case SHORT:
            return Field.nullable(name, Types.MinorType.SMALLINT.getType());
          case INT:
            return Field.nullable(name, Types.MinorType.INT.getType());
          case LONG:
            return Field.nullable(name, Types.MinorType.BIGINT.getType());
          case FLOAT:
            return Field.nullable(name, Types.MinorType.FLOAT4.getType());
          case DOUBLE:
            return Field.nullable(name, Types.MinorType.FLOAT8.getType());
          case STRING:
            return Field.nullable(name, Types.MinorType.VARCHAR.getType());
          case DATE:
            return Field.nullable(name, Types.MinorType.DATEDAY.getType());
          case TIMESTAMP:
            return Field.nullable(name, Types.MinorType.TIMESTAMPMILLI.getType());
          case TIMESTAMPLOCALTZ:
            final TimestampLocalTZTypeInfo timestampLocalTZTypeInfo =
                (TimestampLocalTZTypeInfo) typeInfo;
            final String timeZone = timestampLocalTZTypeInfo.getTimeZone().toString();
            return Field.nullable(name, new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZone));
          case BINARY:
            return Field.nullable(name, Types.MinorType.VARBINARY.getType());
          case DECIMAL:
            final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            final int precision = decimalTypeInfo.precision();
            final int scale = decimalTypeInfo.scale();
            return Field.nullable(name, new ArrowType.Decimal(precision, scale));
          case VARCHAR:
            return Field.nullable(name, Types.MinorType.VARCHAR.getType());
          case CHAR:
            return Field.nullable(name, Types.MinorType.VARCHAR.getType());
          case INTERVAL_YEAR_MONTH:
            return Field.nullable(name, Types.MinorType.INTERVALYEAR.getType());
          case INTERVAL_DAY_TIME:
            return Field.nullable(name, Types.MinorType.INTERVALDAY.getType());
          default:
            throw new IllegalArgumentException();
        }
      case LIST:
        final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        final TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
        return new Field(name, FieldType.nullable(Types.MinorType.LIST.getType()),
            Lists.newArrayList(toField(DEFAULT_ARROW_FIELD_NAME, elementTypeInfo)));
      case STRUCT:
        final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
        final List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        final List<Field> structFields = Lists.newArrayList();
        final int structSize = fieldNames.size();
        for (int i = 0; i < structSize; i++) {
          structFields.add(toField(fieldNames.get(i), fieldTypeInfos.get(i)));
        }
        return new Field(name, FieldType.nullable(Types.MinorType.MAP.getType()), structFields);
      case UNION:
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final List<Field> unionFields = Lists.newArrayList();
        final int unionSize = unionFields.size();
        for (int i = 0; i < unionSize; i++) {
          unionFields.add(toField(DEFAULT_ARROW_FIELD_NAME, objectTypeInfos.get(i)));
        }
        return new Field(name, FieldType.nullable(Types.MinorType.UNION.getType()), unionFields);
      case MAP:
        final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        final TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
        final TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();

        final StructTypeInfo mapStructTypeInfo = new StructTypeInfo();
        mapStructTypeInfo.setAllStructFieldNames(Lists.newArrayList("keys", "values"));
        mapStructTypeInfo.setAllStructFieldTypeInfos(
            Lists.newArrayList(keyTypeInfo, valueTypeInfo));

        final ListTypeInfo mapListStructTypeInfo = new ListTypeInfo();
        mapListStructTypeInfo.setListElementTypeInfo(mapStructTypeInfo);

        return toField(name, mapListStructTypeInfo);
      default:
        throw new IllegalArgumentException();
    }
  }

  private static ListColumnVector toStructListVector(MapColumnVector mapVector) {
    final StructColumnVector structVector;
    final ListColumnVector structListVector;
    structVector = new StructColumnVector();
    structVector.fields = new ColumnVector[] {mapVector.keys, mapVector.values};
    structListVector = new ListColumnVector();
    structListVector.child = structVector;
    System.arraycopy(mapVector.offsets, 0, structListVector.offsets, 0, mapVector.childCount);
    System.arraycopy(mapVector.lengths, 0, structListVector.lengths, 0, mapVector.childCount);
    structListVector.childCount = mapVector.childCount;
    structListVector.isRepeating = mapVector.isRepeating;
    structListVector.noNulls = mapVector.noNulls;
    return structListVector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ArrowWrapperWritable.class;
  }

  @Override
  public ArrowWrapperWritable serialize(Object obj, ObjectInspector objInspector) {
    return serializer.serialize(obj, objInspector);
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    return deserializer.deserialize(writable);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return rowObjectInspector;
  }
}
