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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAssignRow;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo;

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

  static final int MILLIS_PER_SECOND = 1_000;
  static final int MICROS_PER_SECOND = 1_000_000;
  static final int NS_PER_SECOND = 1_000_000_000;

  static final int NS_PER_MILLIS = NS_PER_SECOND / MILLIS_PER_SECOND;
  static final int NS_PER_MICROS = NS_PER_SECOND / MICROS_PER_SECOND;
  static final int MICROS_PER_MILLIS = MICROS_PER_SECOND / MILLIS_PER_SECOND;
  static final int SECOND_PER_DAY = 24 * 60 * 60;

  BufferAllocator rootAllocator;
  StructTypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;
  Configuration conf;

  private Serializer serializer;
  private Deserializer deserializer;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    this.conf = conf;


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

  }

  private static Field toField(String name, TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
          case BOOLEAN:
            return Field.nullable(name, MinorType.BIT.getType());
          case BYTE:
            return Field.nullable(name, MinorType.TINYINT.getType());
          case SHORT:
            return Field.nullable(name, MinorType.SMALLINT.getType());
          case INT:
            return Field.nullable(name, MinorType.INT.getType());
          case LONG:
            return Field.nullable(name, MinorType.BIGINT.getType());
          case FLOAT:
            return Field.nullable(name, MinorType.FLOAT4.getType());
          case DOUBLE:
            return Field.nullable(name, MinorType.FLOAT8.getType());
          case STRING:
          case VARCHAR:
          case CHAR:
            return Field.nullable(name, MinorType.VARCHAR.getType());
          case DATE:
            return Field.nullable(name, MinorType.DATEDAY.getType());
          case TIMESTAMP:
            return Field.nullable(name, MinorType.TIMESTAMPMILLI.getType());
          case TIMESTAMPLOCALTZ:
            final TimestampLocalTZTypeInfo timestampLocalTZTypeInfo =
                (TimestampLocalTZTypeInfo) typeInfo;
            final String timeZone = timestampLocalTZTypeInfo.getTimeZone().toString();
            return Field.nullable(name, new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZone));
          case BINARY:
            return Field.nullable(name, MinorType.VARBINARY.getType());
          case DECIMAL:
            final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            final int precision = decimalTypeInfo.precision();
            final int scale = decimalTypeInfo.scale();
            return Field.nullable(name, new ArrowType.Decimal(precision, scale));
          case INTERVAL_YEAR_MONTH:
            return Field.nullable(name, MinorType.INTERVALYEAR.getType());
          case INTERVAL_DAY_TIME:
            return Field.nullable(name, MinorType.INTERVALDAY.getType());
          default:
            throw new IllegalArgumentException();
        }
      case LIST:
        final ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        final TypeInfo elementTypeInfo = listTypeInfo.getListElementTypeInfo();
        return new Field(name, FieldType.nullable(MinorType.LIST.getType()),
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
        return new Field(name, FieldType.nullable(MinorType.MAP.getType()), structFields);
      case UNION:
        final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
        final List<Field> unionFields = Lists.newArrayList();
        final int unionSize = unionFields.size();
        for (int i = 0; i < unionSize; i++) {
          unionFields.add(toField(DEFAULT_ARROW_FIELD_NAME, objectTypeInfos.get(i)));
        }
        return new Field(name, FieldType.nullable(MinorType.UNION.getType()), unionFields);
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

  static ListTypeInfo toStructListTypeInfo(MapTypeInfo mapTypeInfo) {
    final StructTypeInfo structTypeInfo = new StructTypeInfo();
    structTypeInfo.setAllStructFieldNames(Lists.newArrayList("keys", "values"));
    structTypeInfo.setAllStructFieldTypeInfos(Lists.newArrayList(
        mapTypeInfo.getMapKeyTypeInfo(), mapTypeInfo.getMapValueTypeInfo()));
    final ListTypeInfo structListTypeInfo = new ListTypeInfo();
    structListTypeInfo.setListElementTypeInfo(structTypeInfo);
    return structListTypeInfo;
  }

  static ListColumnVector toStructListVector(MapColumnVector mapVector) {
    final StructColumnVector structVector;
    final ListColumnVector structListVector;
    structVector = new StructColumnVector();
    structVector.fields = new ColumnVector[] {mapVector.keys, mapVector.values};
    structListVector = new ListColumnVector();
    structListVector.child = structVector;
    structListVector.childCount = mapVector.childCount;
    structListVector.isRepeating = mapVector.isRepeating;
    structListVector.noNulls = mapVector.noNulls;
    System.arraycopy(mapVector.offsets, 0, structListVector.offsets, 0, mapVector.childCount);
    System.arraycopy(mapVector.lengths, 0, structListVector.lengths, 0, mapVector.childCount);
    return structListVector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ArrowWrapperWritable.class;
  }

  @Override
  public ArrowWrapperWritable serialize(Object obj, ObjectInspector objInspector) {
    if(serializer == null) {
      try {
        rootAllocator = RootAllocatorFactory.INSTANCE.getRootAllocator(conf);
        serializer = new Serializer(this);
      } catch(Exception e) {
        LOG.error("Unable to initialize serializer for ArrowColumnarBatchSerDe");
        throw new RuntimeException(e);
      }
    }
    return serializer.serialize(obj, objInspector);
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    if(deserializer == null) {
      try {
        rootAllocator = RootAllocatorFactory.INSTANCE.getRootAllocator(conf);
        deserializer = new Deserializer(this);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
    return deserializer.deserialize(writable);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return rowObjectInspector;
  }
}
