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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;

/**
 * Convert an Avro Schema to a Hive TypeInfo
 */
class SchemaToTypeInfo {
  // Conversion of Avro primitive types to Hive primitive types
  // Avro             Hive
  // Null
  // boolean          boolean    check
  // int              int        check
  // long             bigint     check
  // float            double     check
  // double           double     check
  // bytes
  // string           string     check
  //                  tinyint
  //                  smallint

  // Map of Avro's primitive types to Hives (for those that are supported by both)
  private static final Map<Schema.Type, TypeInfo> primitiveTypeToTypeInfo = initTypeMap();
  private static Map<Schema.Type, TypeInfo> initTypeMap() {
    Map<Schema.Type, TypeInfo> theMap = new Hashtable<Schema.Type, TypeInfo>();
    theMap.put(STRING, TypeInfoFactory.getPrimitiveTypeInfo("string"));
    theMap.put(INT, TypeInfoFactory.getPrimitiveTypeInfo("int"));
    theMap.put(BOOLEAN, TypeInfoFactory.getPrimitiveTypeInfo("boolean"));
    theMap.put(LONG, TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    theMap.put(FLOAT, TypeInfoFactory.getPrimitiveTypeInfo("float"));
    theMap.put(DOUBLE, TypeInfoFactory.getPrimitiveTypeInfo("double"));
    theMap.put(NULL, TypeInfoFactory.getPrimitiveTypeInfo("void"));
    return Collections.unmodifiableMap(theMap);
  }

  /**
   * Generate a list of of TypeInfos from an Avro schema.  This method is
   * currently public due to some weirdness in deserializing unions, but
   * will be made private once that is resolved.
   * @param schema Schema to generate field types for
   * @return List of TypeInfos, each element of which is a TypeInfo derived
   *         from the schema.
   * @throws AvroSerdeException for problems during conversion.
   */
  public static List<TypeInfo> generateColumnTypes(Schema schema) throws AvroSerdeException {
    List<Schema.Field> fields = schema.getFields();

    List<TypeInfo> types = new ArrayList<TypeInfo>(fields.size());

    for (Schema.Field field : fields) {
      types.add(generateTypeInfo(field.schema()));
    }

    return types;
  }

  static InstanceCache<Schema, TypeInfo> typeInfoCache = new InstanceCache<Schema, TypeInfo>() {
                                  @Override
                                  protected TypeInfo makeInstance(Schema s) throws AvroSerdeException {
                                    return generateTypeInfoWorker(s);
                                  }
                                };
  /**
   * Convert an Avro Schema into an equivalent Hive TypeInfo.
   * @param schema to record. Must be of record type.
   * @return TypeInfo matching the Avro schema
   * @throws AvroSerdeException for any problems during conversion.
   */
  public static TypeInfo generateTypeInfo(Schema schema) throws AvroSerdeException {
    return typeInfoCache.retrieve(schema);
  }

  private static TypeInfo generateTypeInfoWorker(Schema schema) throws AvroSerdeException {
    // Avro requires NULLable types to be defined as unions of some type T
    // and NULL.  This is annoying and we're going to hide it from the user.
    if(AvroSerdeUtils.isNullableType(schema))
      return generateTypeInfo(AvroSerdeUtils.getOtherTypeFromNullableType(schema));

    Schema.Type type = schema.getType();

    if(primitiveTypeToTypeInfo.containsKey(type))
      return primitiveTypeToTypeInfo.get(type);

    switch(type) {
      case BYTES:  return generateBytesTypeInfo(schema);
      case RECORD: return generateRecordTypeInfo(schema);
      case MAP:    return generateMapTypeInfo(schema);
      case ARRAY:  return generateArrayTypeInfo(schema);
      case UNION:  return generateUnionTypeInfo(schema);
      case ENUM:   return generateEnumTypeInfo(schema);
      case FIXED:  return generateFixedTypeInfo(schema);
      default:     throw new AvroSerdeException("Do not yet support: " + schema);
    }
  }

  private static TypeInfo generateRecordTypeInfo(Schema schema) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.RECORD);

    List<Schema.Field> fields = schema.getFields();
    List<String> fieldNames = new ArrayList<String>(fields.size());
    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>(fields.size());

    for(int i = 0; i < fields.size(); i++) {
      fieldNames.add(i, fields.get(i).name());
      typeInfos.add(i, generateTypeInfo(fields.get(i).schema()));
    }

    return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
  }

  /**
   * Generate a TypeInfo for an Avro Map.  This is made slightly simpler in that
   * Avro only allows maps with strings for keys.
   */
  private static TypeInfo generateMapTypeInfo(Schema schema) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.MAP);
    Schema valueType = schema.getValueType();
    TypeInfo ti = generateTypeInfo(valueType);

    return TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("string"), ti);
  }

  private static TypeInfo generateArrayTypeInfo(Schema schema) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.ARRAY);
    Schema itemsType = schema.getElementType();
    TypeInfo itemsTypeInfo = generateTypeInfo(itemsType);

    return TypeInfoFactory.getListTypeInfo(itemsTypeInfo);
  }

  private static TypeInfo generateUnionTypeInfo(Schema schema) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.UNION);
    List<Schema> types = schema.getTypes();


    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>(types.size());

    for(Schema type : types) {
      typeInfos.add(generateTypeInfo(type));
    }

    return TypeInfoFactory.getUnionTypeInfo(typeInfos);
  }

  // Hive doesn't have an Enum type, so we're going to treat them as Strings.
  // During the deserialize/serialize stage we'll check for enumness and
  // convert as such.
  private static TypeInfo generateEnumTypeInfo(Schema schema) {
    assert schema.getType().equals(Schema.Type.ENUM);

    return TypeInfoFactory.getPrimitiveTypeInfo("string");
  }

  // Hive doesn't have a Fixed type, so we're going to treat them as arrays of
  // bytes
  // TODO: Make note in documentation that Hive sends these out as signed bytes.
  private static final TypeInfo FIXED_AND_BYTES_EQUIV =
          TypeInfoFactory.getListTypeInfo(TypeInfoFactory.byteTypeInfo);
  private static TypeInfo generateFixedTypeInfo(Schema schema) {
    assert schema.getType().equals(Schema.Type.FIXED);

    return FIXED_AND_BYTES_EQUIV;
  }

  // Avro considers bytes to be a primitive type, but Hive doesn't.  We'll
  // convert them to a list of bytes, just like Fixed.  Sigh.
  private static TypeInfo generateBytesTypeInfo(Schema schema) {
    assert schema.getType().equals(Schema.Type.BYTES);
    return FIXED_AND_BYTES_EQUIV;
  }
}
