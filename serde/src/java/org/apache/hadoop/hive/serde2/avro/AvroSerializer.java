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
package org.apache.hadoop.hive.serde2.avro;

import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.common.type.CalendarUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

class AvroSerializer {

  /**
   * The Schema to use when serializing Map keys.
   * Since we're sharing this across Serializer instances, it must be immutable;
   * any properties need to be added in a static initializer.
   */
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private AvroGenericRecordWritable cache = new AvroGenericRecordWritable();
  private boolean defaultProleptic;
  private final boolean legacyConversion;

  AvroSerializer() {
    this.legacyConversion = ConfVars.HIVE_AVRO_TIMESTAMP_WRITE_LEGACY_CONVERSION_ENABLED.defaultBoolVal;
  }

  AvroSerializer(Configuration configuration) {
    this.defaultProleptic = HiveConf.getBoolVar(
        configuration, ConfVars.HIVE_AVRO_PROLEPTIC_GREGORIAN);
    this.legacyConversion =
        HiveConf.getBoolVar(configuration, ConfVars.HIVE_AVRO_TIMESTAMP_WRITE_LEGACY_CONVERSION_ENABLED);
  }

  // Hive is pretty simple (read: stupid) in writing out values via the serializer.
  // We're just going to go through, matching indices.  Hive formats normally
  // handle mismatches with null.  We don't have that option, so instead we'll
  // end up throwing an exception for invalid records.
  public Writable serialize(Object o, ObjectInspector objectInspector, List<String> columnNames, List<TypeInfo> columnTypes, Schema schema) throws AvroSerdeException {
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    GenericData.Record record = new GenericData.Record(schema);

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    if(outputFieldRefs.size() != columnNames.size()) {
      throw new AvroSerdeException("Number of input columns was different than output columns (in = " + columnNames.size() + " vs out = " + outputFieldRefs.size());
    }

    int size = schema.getFields().size();
    if(outputFieldRefs.size() != size) {
      throw new AvroSerdeException("Hive passed in a different number of fields than the schema expected: (Hive wanted " + outputFieldRefs.size() +", Avro expected " + schema.getFields().size());
    }

    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);

    for(int i  = 0; i < size; i++) {
      Field field = schema.getFields().get(i);
      TypeInfo typeInfo = columnTypes.get(i);
      StructField structFieldRef = allStructFieldRefs.get(i);
      Object structFieldData = structFieldsDataAsList.get(i);
      ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();

      Object val = serialize(typeInfo, fieldOI, structFieldData, field.schema());
      record.put(field.name(), val);
    }

    if(!GenericData.get().validate(schema, record)) {
      throw new SerializeToAvroException(schema, record);
    }

    cache.setRecord(record);

    return cache;
  }

  private Object serialize(TypeInfo typeInfo, ObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    if(null == structFieldData) {
      return null;
    }

    if(AvroSerdeUtils.isNullableType(schema)) {
      schema = AvroSerdeUtils.getOtherTypeFromNullableType(schema);
    }
    /* Because we use Hive's 'string' type when Avro calls for enum, we have to expressly check for enum-ness */
    if(Schema.Type.ENUM.equals(schema.getType())) {
      assert fieldOI instanceof PrimitiveObjectInspector;
      return serializeEnum(typeInfo, (PrimitiveObjectInspector) fieldOI, structFieldData, schema);
    }
    switch(typeInfo.getCategory()) {
      case PRIMITIVE:
        assert fieldOI instanceof PrimitiveObjectInspector;
        return serializePrimitive(typeInfo, (PrimitiveObjectInspector) fieldOI, structFieldData, schema);
      case MAP:
        assert fieldOI instanceof MapObjectInspector;
        assert typeInfo instanceof MapTypeInfo;
        return serializeMap((MapTypeInfo) typeInfo, (MapObjectInspector) fieldOI, structFieldData, schema);
      case LIST:
        assert fieldOI instanceof ListObjectInspector;
        assert typeInfo instanceof ListTypeInfo;
        return serializeList((ListTypeInfo) typeInfo, (ListObjectInspector) fieldOI, structFieldData, schema);
      case UNION:
        assert fieldOI instanceof UnionObjectInspector;
        assert typeInfo instanceof UnionTypeInfo;
        return serializeUnion((UnionTypeInfo) typeInfo, (UnionObjectInspector) fieldOI, structFieldData, schema);
      case STRUCT:
        assert fieldOI instanceof StructObjectInspector;
        assert typeInfo instanceof StructTypeInfo;
        return serializeStruct((StructTypeInfo) typeInfo, (StructObjectInspector) fieldOI, structFieldData, schema);
      default:
        throw new AvroSerdeException("Ran out of TypeInfo Categories: " + typeInfo.getCategory());
    }
  }

  /** private cache to avoid lots of EnumSymbol creation while serializing.
   *  Two levels because the enum symbol is specific to a schema.
   *  Object because we want to avoid the overhead of repeated toString calls while maintaining compatibility.
   *  Provided there are few enum types per record, and few symbols per enum, memory use should be moderate.
   *  eg 20 types with 50 symbols each as length-10 Strings should be on the order of 100KB per AvroSerializer.
   */
  final InstanceCache<Schema, InstanceCache<Object, GenericEnumSymbol>> enums
      = new InstanceCache<Schema, InstanceCache<Object, GenericEnumSymbol>>() {
          @Override
          protected InstanceCache<Object, GenericEnumSymbol> makeInstance(final Schema schema,
                     Set<Schema> seenSchemas) {
            return new InstanceCache<Object, GenericEnumSymbol>() {
              @Override
              protected GenericEnumSymbol makeInstance(Object seed,
                             Set<Object> seenSchemas) {
                return new GenericData.EnumSymbol(schema, seed.toString());
              }
            };
          }
        };

  private Object serializeEnum(TypeInfo typeInfo, PrimitiveObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    return enums.retrieve(schema).retrieve(serializePrimitive(typeInfo, fieldOI, structFieldData, schema));
  }

  private Object serializeStruct(StructTypeInfo typeInfo, StructObjectInspector ssoi, Object o, Schema schema) throws AvroSerdeException {
    int size = schema.getFields().size();
    List<? extends StructField> allStructFieldRefs = ssoi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = ssoi.getStructFieldsDataAsList(o);
    GenericData.Record record = new GenericData.Record(schema);
    List<TypeInfo> allStructFieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();

    for(int i  = 0; i < size; i++) {
      Field field = schema.getFields().get(i);
      TypeInfo colTypeInfo = allStructFieldTypeInfos.get(i);
      StructField structFieldRef = allStructFieldRefs.get(i);
      Object structFieldData = structFieldsDataAsList.get(i);
      ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();

      Object val = serialize(colTypeInfo, fieldOI, structFieldData, field.schema());
      record.put(field.name(), val);
    }
    return record;
  }

  private Object serializePrimitive(TypeInfo typeInfo, PrimitiveObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    switch(fieldOI.getPrimitiveCategory()) {
    case BINARY:
      if (schema.getType() == Type.BYTES){
        return AvroSerdeUtils.getBufferFromBytes((byte[])fieldOI.getPrimitiveJavaObject(structFieldData));
      } else if (schema.getType() == Type.FIXED){
        Fixed fixed = new GenericData.Fixed(schema, (byte[])fieldOI.getPrimitiveJavaObject(structFieldData));
        return fixed;
      } else {
        throw new AvroSerdeException("Unexpected Avro schema for Binary TypeInfo: " + schema.getType());
      }
    case DECIMAL:
      HiveDecimal dec = (HiveDecimal)fieldOI.getPrimitiveJavaObject(structFieldData);
      return AvroSerdeUtils.getBufferFromDecimal(dec, ((DecimalTypeInfo)typeInfo).scale());
    case CHAR:
      HiveChar ch = (HiveChar)fieldOI.getPrimitiveJavaObject(structFieldData);
      return ch.getStrippedValue();
    case VARCHAR:
      HiveVarchar vc = (HiveVarchar)fieldOI.getPrimitiveJavaObject(structFieldData);
      return vc.getValue();
    case DATE:
      Date date = ((DateObjectInspector)fieldOI).getPrimitiveJavaObject(structFieldData);
      return defaultProleptic ? date.toEpochDay() :
          CalendarUtils.convertDateToHybrid(date.toEpochDay());
    case TIMESTAMP:
      Timestamp timestamp =
        ((TimestampObjectInspector) fieldOI).getPrimitiveJavaObject(structFieldData);
      long millis = defaultProleptic ? timestamp.toEpochMilli() :
          CalendarUtils.convertTimeToHybrid(timestamp.toEpochMilli());
      timestamp = TimestampTZUtil.convertTimestampToZone(
          Timestamp.ofEpochMilli(millis), TimeZone.getDefault().toZoneId(), ZoneOffset.UTC, legacyConversion);
      return timestamp.toEpochMilli();
    case UNKNOWN:
      throw new AvroSerdeException("Received UNKNOWN primitive category.");
    case VOID:
      return null;
    default: // All other primitive types are simple
      return fieldOI.getPrimitiveJavaObject(structFieldData);
    }
  }

  private Object serializeUnion(UnionTypeInfo typeInfo, UnionObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    byte tag = fieldOI.getTag(structFieldData);

    // Invariant that Avro's tag ordering must match Hive's.
    return serialize(typeInfo.getAllUnionObjectTypeInfos().get(tag),
                     fieldOI.getObjectInspectors().get(tag),
                     fieldOI.getField(structFieldData),
                     schema.getTypes().get(tag));
  }

  private Object serializeList(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    List<?> list = fieldOI.getList(structFieldData);
    List<Object> deserialized = new GenericData.Array<Object>(list.size(), schema);

    TypeInfo listElementTypeInfo = typeInfo.getListElementTypeInfo();
    ObjectInspector listElementObjectInspector = fieldOI.getListElementObjectInspector();
    Schema elementType = schema.getElementType();

    for(int i = 0; i < list.size(); i++) {
      deserialized.add(i, serialize(listElementTypeInfo, listElementObjectInspector, list.get(i), elementType));
    }

    return deserialized;
  }

  private Object serializeMap(MapTypeInfo typeInfo, MapObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    // Avro only allows maps with string keys
    if(!mapHasStringKey(fieldOI.getMapKeyObjectInspector())) {
      throw new AvroSerdeException("Avro only supports maps with keys as Strings.  Current Map is: " + typeInfo.toString());
    }

    ObjectInspector mapKeyObjectInspector = fieldOI.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = fieldOI.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = typeInfo.getMapKeyTypeInfo();
    TypeInfo mapValueTypeInfo = typeInfo.getMapValueTypeInfo();
    Map<?,?> map = fieldOI.getMap(structFieldData);
    Schema valueType = schema.getValueType();

    Map<Object, Object> deserialized = new LinkedHashMap<Object, Object>(fieldOI.getMapSize(structFieldData));

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      deserialized.put(serialize(mapKeyTypeInfo, mapKeyObjectInspector, entry.getKey(), STRING_SCHEMA),
                       serialize(mapValueTypeInfo, mapValueObjectInspector, entry.getValue(), valueType));
    }

    return deserialized;
  }

  private boolean mapHasStringKey(ObjectInspector mapKeyObjectInspector) {
    return mapKeyObjectInspector instanceof PrimitiveObjectInspector &&
        ((PrimitiveObjectInspector) mapKeyObjectInspector)
            .getPrimitiveCategory()
            .equals(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  }

  /**
   * Thrown when, during serialization of a Hive row to an Avro record, Avro
   * cannot verify the converted row to the record's schema.
   */
  public static class SerializeToAvroException extends AvroSerdeException {
    final private Schema schema;
    final private GenericData.Record record;

    public SerializeToAvroException(Schema schema, GenericData.Record record) {
      this.schema = schema;
      this.record = record;
    }

    @Override
    public String toString() {
      return "Avro could not validate record against schema (record = " + record
          + ") (schema = "+schema.toString(false) + ")";
    }
  }
}
