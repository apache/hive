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

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.STRING;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.type.DecimalMetastoreTypeInfo;
import org.apache.hadoop.hive.metastore.type.MetastoreTypeInfo;
import org.apache.hadoop.hive.metastore.type.MetastoreTypeInfoFactory;
import org.apache.hadoop.hive.metastore.utils.AvroSchemaUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convert an Avro Schema to a Hive TypeInfo. This class is copied from Hive source code for
 * standalone metastore
 */
public class SchemaToTypeInfo {
  // Conversion of Avro primitive types to Hive primitive types
  // Avro             Hive
  // Null
  // boolean          boolean    check
  // int              int        check
  // long             bigint     check
  // float            double     check
  // double           double     check
  // bytes            binary     check
  // fixed            binary     check
  // string           string     check
  //                  tinyint
  //                  smallint

  // Map of Avro's primitive types to Hives (for those that are supported by both)
  private static final Map<Schema.Type,  MetastoreTypeInfo> primitiveTypeToTypeInfo = initTypeMap();
  private static Map<Schema.Type,  MetastoreTypeInfo> initTypeMap() {
    Map<Schema.Type,  MetastoreTypeInfo> theMap = new Hashtable<Schema.Type,  MetastoreTypeInfo>();
    theMap.put(NULL,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("void"));
    theMap.put(BOOLEAN,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("boolean"));
    theMap.put(INT,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("int"));
    theMap.put(LONG,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    theMap.put(FLOAT,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("float"));
    theMap.put(DOUBLE,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("double"));
    theMap.put(BYTES,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("binary"));
    theMap.put(FIXED,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("binary"));
    theMap.put(STRING,  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("string"));
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
  public static List< MetastoreTypeInfo> generateColumnTypes(Schema schema) throws AvroSerdeException {
    return generateColumnTypes (schema, null);
  }

  /**
   * Generate a list of of TypeInfos from an Avro schema.  This method is
   * currently public due to some weirdness in deserializing unions, but
   * will be made private once that is resolved.
   * @param schema Schema to generate field types for
   * @param seenSchemas stores schemas processed in the parsing done so far,
   *         helping to resolve circular references in the schema
   * @return List of TypeInfos, each element of which is a  MetastoreTypeInfo derived
   *         from the schema.
   * @throws AvroSerdeException for problems during conversion.
   */
  public static List< MetastoreTypeInfo> generateColumnTypes(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    List<Schema.Field> fields = schema.getFields();

    List< MetastoreTypeInfo> types = new ArrayList< MetastoreTypeInfo>(fields.size());

    for (Schema.Field field : fields) {
      types.add(generateTypeInfo(field.schema(), seenSchemas));
    }

    return types;
  }

  static InstanceCache<Schema,  MetastoreTypeInfo> typeInfoCache = new InstanceCache<Schema,  MetastoreTypeInfo>() {
                                  @Override
                                  protected  MetastoreTypeInfo makeInstance(Schema s,
                                      Set<Schema> seenSchemas)
                                      throws AvroSerdeException {
                                    return generateTypeInfoWorker(s, seenSchemas);
                                  }
                                };
  /**
   * Convert an Avro Schema into an equivalent Hive  MetastoreTypeInfo.
   * @param schema to record. Must be of record type.
   * @param seenSchemas stores schemas processed in the parsing done so far,
   *         helping to resolve circular references in the schema
   * @return  MetastoreTypeInfo matching the Avro schema
   * @throws AvroSerdeException for any problems during conversion.
   */
  public static  MetastoreTypeInfo generateTypeInfo(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    // For bytes type, it can be mapped to decimal.
    Schema.Type type = schema.getType();
    if (type == BYTES && AvroSerDeConstants.DECIMAL_TYPE_NAME
      .equalsIgnoreCase(schema.getProp(AvroSerDeConstants.AVRO_PROP_LOGICAL_TYPE))) {
      int precision = 0;
      int scale = 0;
      try {
        precision = schema.getJsonProp(AvroSerDeConstants.AVRO_PROP_PRECISION).getIntValue();
        scale = schema.getJsonProp(AvroSerDeConstants.AVRO_PROP_SCALE).getIntValue();
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain scale value from file schema: " + schema, ex);
      }

      try {
        DecimalMetastoreTypeInfo.validateParameter(precision, scale);
      } catch (Exception ex) {
        throw new AvroSerdeException("Invalid precision or scale for decimal type", ex);
      }

      return  MetastoreTypeInfoFactory.getDecimalTypeInfo(precision, scale);
    }

    if (type == STRING &&
        AvroSerDeConstants.CHAR_TYPE_NAME.equalsIgnoreCase(schema.getProp(AvroSerDeConstants.AVRO_PROP_LOGICAL_TYPE))) {
      int maxLength = 0;
      try {
        maxLength = schema.getJsonProp(AvroSerDeConstants.AVRO_PROP_MAX_LENGTH).getValueAsInt();
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
      }
      return  MetastoreTypeInfoFactory.getCharTypeInfo(maxLength);
    }

    if (type == STRING && AvroSerDeConstants.VARCHAR_TYPE_NAME
      .equalsIgnoreCase(schema.getProp(AvroSerDeConstants.AVRO_PROP_LOGICAL_TYPE))) {
      int maxLength = 0;
      try {
        maxLength = schema.getJsonProp(AvroSerDeConstants.AVRO_PROP_MAX_LENGTH).getValueAsInt();
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
      }
      return  MetastoreTypeInfoFactory.getVarcharTypeInfo(maxLength);
    }

    if (type == INT &&
        AvroSerDeConstants.DATE_TYPE_NAME.equals(schema.getProp(AvroSerDeConstants.AVRO_PROP_LOGICAL_TYPE))) {
      //in case AvroSerDeConstants.DATE_TYPE_NAME matches with ColumnType.DATE_TYPE_NAME this will
      //error out. This code works since they both are expected to be same. If this assumption is broken
      //we need to map avro's date type to DateMetastoreTypeInfo
      return  MetastoreTypeInfoFactory.getPrimitiveTypeInfo(AvroSerDeConstants.DATE_TYPE_NAME);
    }

    if (type == LONG &&
        AvroSerDeConstants.AVRO_TIMESTAMP_TYPE_NAME.equals(schema.getProp(AvroSerDeConstants.AVRO_PROP_LOGICAL_TYPE))) {
      //The AVRO's timestamp type is different than the metastore's timestamp type. Use ColumnType.TIMESTAMP_TYPE_NAME
      return  MetastoreTypeInfoFactory.getPrimitiveTypeInfo(ColumnType.TIMESTAMP_TYPE_NAME);
    }

    return typeInfoCache.retrieve(schema, seenSchemas);
  }

  private static  MetastoreTypeInfo generateTypeInfoWorker(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    // Avro requires NULLable types to be defined as unions of some type T
    // and NULL.  This is annoying and we're going to hide it from the user.
    if(AvroSchemaUtils.isNullableType(schema)) {
      return generateTypeInfo(
          AvroSchemaUtils.getOtherTypeFromNullableType(schema), seenSchemas);
    }

    Schema.Type type = schema.getType();
    if(primitiveTypeToTypeInfo.containsKey(type)) {
      return primitiveTypeToTypeInfo.get(type);
    }

    switch(type) {
      case RECORD: return generateRecordTypeInfo(schema, seenSchemas);
      case MAP:    return generateMapTypeInfo(schema, seenSchemas);
      case ARRAY:  return generateArrayTypeInfo(schema, seenSchemas);
      case UNION:  return generateUnionTypeInfo(schema, seenSchemas);
      case ENUM:   return generateEnumTypeInfo(schema);
      default:     throw new AvroSerdeException("Do not yet support: " + schema);
    }
  }

  private static  MetastoreTypeInfo generateRecordTypeInfo(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.RECORD);

    if (seenSchemas == null) {
        seenSchemas = Collections.newSetFromMap(new IdentityHashMap<Schema, Boolean>());
    } else if (seenSchemas.contains(schema)) {
      throw new AvroSerdeException(
          "Recursive schemas are not supported. Recursive schema was " + schema
              .getFullName());
    }
    seenSchemas.add(schema);

    List<Schema.Field> fields = schema.getFields();
    List<String> fieldNames = new ArrayList<String>(fields.size());
    List< MetastoreTypeInfo> typeInfos = new ArrayList< MetastoreTypeInfo>(fields.size());

    for(int i = 0; i < fields.size(); i++) {
      fieldNames.add(i, fields.get(i).name());
      typeInfos.add(i, generateTypeInfo(fields.get(i).schema(), seenSchemas));
    }

    return  MetastoreTypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
  }

  /**
   * Generate a  MetastoreTypeInfo for an Avro Map.  This is made slightly simpler in that
   * Avro only allows maps with strings for keys.
   */
  private static  MetastoreTypeInfo generateMapTypeInfo(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.MAP);
    Schema valueType = schema.getValueType();
     MetastoreTypeInfo ti = generateTypeInfo(valueType, seenSchemas);

    return  MetastoreTypeInfoFactory.getMapTypeInfo( MetastoreTypeInfoFactory.getPrimitiveTypeInfo("string"), ti);
  }

  private static  MetastoreTypeInfo generateArrayTypeInfo(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.ARRAY);
    Schema itemsType = schema.getElementType();
     MetastoreTypeInfo itemsTypeInfo = generateTypeInfo(itemsType, seenSchemas);

    return  MetastoreTypeInfoFactory.getListTypeInfo(itemsTypeInfo);
  }

  private static  MetastoreTypeInfo generateUnionTypeInfo(Schema schema,
      Set<Schema> seenSchemas) throws AvroSerdeException {
    assert schema.getType().equals(Schema.Type.UNION);
    List<Schema> types = schema.getTypes();


    List< MetastoreTypeInfo> typeInfos = new ArrayList< MetastoreTypeInfo>(types.size());

    for(Schema type : types) {
      typeInfos.add(generateTypeInfo(type, seenSchemas));
    }

    return  MetastoreTypeInfoFactory.getUnionTypeInfo(typeInfos);
  }

  // Hive doesn't have an Enum type, so we're going to treat them as Strings.
  // During the deserialize/serialize stage we'll check for enumness and
  // convert as such.
  private static MetastoreTypeInfo generateEnumTypeInfo(Schema schema) {
    assert schema.getType().equals(Schema.Type.ENUM);

    return  MetastoreTypeInfoFactory.getPrimitiveTypeInfo("string");
  }
}
