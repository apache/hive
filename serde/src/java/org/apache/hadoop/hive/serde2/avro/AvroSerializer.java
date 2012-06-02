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


import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.FIXED;

class AvroSerializer {
  private static final Log LOG = LogFactory.getLog(AvroSerializer.class);

  AvroGenericRecordWritable cache = new AvroGenericRecordWritable();

  // Hive is pretty simple (read: stupid) in writing out values via the serializer.
  // We're just going to go through, matching indices.  Hive formats normally
  // handle mismatches with null.  We don't have that option, so instead we'll
  // end up throwing an exception for invalid records.
  public Writable serialize(Object o, ObjectInspector objectInspector, List<String> columnNames, List<TypeInfo> columnTypes, Schema schema) throws AvroSerdeException {
    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    GenericData.Record record = new GenericData.Record(schema);

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    if(outputFieldRefs.size() != columnNames.size())
      throw new AvroSerdeException("Number of input columns was different than output columns (in = " + columnNames.size() + " vs out = " + outputFieldRefs.size());

    int size = schema.getFields().size();
    if(outputFieldRefs.size() != size) // Hive does this check for us, so we should be ok.
      throw new AvroSerdeException("Hive passed in a different number of fields than the schema expected: (Hive wanted " + outputFieldRefs.size() +", Avro expected " + schema.getFields().size());

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

    if(!GenericData.get().validate(schema, record))
      throw new SerializeToAvroException(schema, record);

    cache.setRecord(record);

    return cache;
  }

  private Object serialize(TypeInfo typeInfo, ObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    switch(typeInfo.getCategory()) {
      case PRIMITIVE:
        assert fieldOI instanceof PrimitiveObjectInspector;
        return serializePrimitive(typeInfo, (PrimitiveObjectInspector) fieldOI, structFieldData);
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

  private Object serializeStruct(StructTypeInfo typeInfo, StructObjectInspector ssoi, Object o, Schema schema) throws AvroSerdeException {
    int size = schema.getFields().size();
    List<? extends StructField> allStructFieldRefs = ssoi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = ssoi.getStructFieldsDataAsList(o);
    GenericData.Record record = new GenericData.Record(schema);
    ArrayList<TypeInfo> allStructFieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();

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

  private Object serializePrimitive(TypeInfo typeInfo, PrimitiveObjectInspector fieldOI, Object structFieldData) throws AvroSerdeException {
    switch(fieldOI.getPrimitiveCategory()) {
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

  // We treat FIXED and BYTES as arrays of tinyints within Hive.  Check
  // if we're dealing with either of these types and thus need to serialize
  // them as their Avro types.
  private boolean isTransformedType(Schema schema) {
    return schema.getType().equals(FIXED) || schema.getType().equals(BYTES);
  }

  private Object serializeTransformedType(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Beginning to transform " + typeInfo + " with Avro schema " + schema.toString(false));
    }
    if(schema.getType().equals(FIXED)) return serializedAvroFixed(typeInfo, fieldOI, structFieldData, schema);
    else return serializeAvroBytes(typeInfo, fieldOI, structFieldData, schema);

  }

  private Object serializeAvroBytes(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    ByteBuffer bb = ByteBuffer.wrap(extraByteArray(fieldOI, structFieldData));
    return bb.rewind();
  }

  private Object serializedAvroFixed(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    return new GenericData.Fixed(schema, extraByteArray(fieldOI, structFieldData));
  }

  // For transforming to BYTES and FIXED, pull out the byte array Avro will want
  private byte[] extraByteArray(ListObjectInspector fieldOI, Object structFieldData) throws AvroSerdeException {
    // Grab a book.  This is going to be slow.
    int listLength = fieldOI.getListLength(structFieldData);
    byte[] bytes = new byte[listLength];
    assert fieldOI.getListElementObjectInspector() instanceof PrimitiveObjectInspector;
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)fieldOI.getListElementObjectInspector();
    List<?> list = fieldOI.getList(structFieldData);

    for(int i = 0; i < listLength; i++) {
      Object b = poi.getPrimitiveJavaObject(list.get(i));
      if(!(b instanceof Byte))
        throw new AvroSerdeException("Attempting to transform to bytes, element was not byte but " + b.getClass().getCanonicalName());
      bytes[i] = (Byte)b;
    }
    return bytes;
  }

  private Object serializeList(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws AvroSerdeException {
    if(isTransformedType(schema))
      return serializeTransformedType(typeInfo, fieldOI, structFieldData, schema);
    
    List<?> list = fieldOI.getList(structFieldData);
    List<Object> deserialized = new ArrayList<Object>(list.size());

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
    if(!mapHasStringKey(fieldOI.getMapKeyObjectInspector()))
      throw new AvroSerdeException("Avro only supports maps with keys as Strings.  Current Map is: " + typeInfo.toString());

    ObjectInspector mapKeyObjectInspector = fieldOI.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = fieldOI.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = typeInfo.getMapKeyTypeInfo();
    TypeInfo mapValueTypeInfo = typeInfo.getMapValueTypeInfo();
    Map<?,?> map = fieldOI.getMap(structFieldData);
    Schema valueType = schema.getValueType();

    Map<Object, Object> deserialized = new Hashtable<Object, Object>(fieldOI.getMapSize(structFieldData));

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      deserialized.put(serialize(mapKeyTypeInfo, mapKeyObjectInspector, entry.getKey(), null), // This works, but is a bit fragile.  Construct a single String schema?
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
