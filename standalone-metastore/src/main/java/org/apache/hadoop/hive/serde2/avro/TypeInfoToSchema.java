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
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.utils.AvroSchemaUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MetastorePrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MetastoreTypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert Hive TypeInfo to an Avro Schema
 */
public class TypeInfoToSchema {

  private long recordCounter = 0;

  /**
   * Converts Hive schema to avro schema
   *
   * @param columnNames Names of the hive columns
   * @param columnTypes Hive Column types
   * @param namespace   Namespace of Avro schema
   * @param name        Avro schema name
   * @param doc         Avro schema doc
   * @return Avro Schema
   */
  public Schema convert(List<String> columnNames, List<TypeInfo> columnTypes,
                        List<String> columnComments, String namespace, String name, String doc) {

    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < columnNames.size(); ++i) {
      final String comment = columnComments.size() > i ? columnComments.get(i) : null;
      final Schema.Field avroField = createAvroField(columnNames.get(i), columnTypes.get(i),
          comment);
      fields.addAll(getFields(avroField));
    }

    if (name == null || name.isEmpty()) {
      name = "baseRecord";
    }

    Schema avroSchema = Schema.createRecord(name, doc, namespace, false);
    avroSchema.setFields(fields);
    return avroSchema;
  }

  private Schema.Field createAvroField(String name, TypeInfo typeInfo, String comment) {
    return new Schema.Field(name, createAvroSchema(typeInfo), comment, null);
  }

  private Schema createAvroSchema(TypeInfo typeInfo) {
    Schema schema = null;
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      schema = createAvroPrimitive(typeInfo);
      break;
    case LIST:
      schema = createAvroArray(typeInfo);
      break;
    case MAP:
      schema = createAvroMap(typeInfo);
      break;
    case STRUCT:
      schema = createAvroRecord(typeInfo);
      break;
    case UNION:
      schema = createAvroUnion(typeInfo);
      break;
    }

    return wrapInUnionWithNull(schema);
  }

  private Schema createAvroPrimitive(TypeInfo typeInfo) {
    Schema schema;
    String baseTypeName = MetastoreTypeInfoUtils.getBaseName(typeInfo.getTypeName());
    switch (baseTypeName) {
      case ColumnType.STRING_TYPE_NAME:
        schema = Schema.create(Schema.Type.STRING);
        break;
      case ColumnType.CHAR_TYPE_NAME:
        schema = AvroSchemaUtils.getSchemaFor("{" +
            "\"type\":\"" + AvroSerDeConstants.AVRO_STRING_TYPE_NAME + "\"," +
            "\"logicalType\":\"" + AvroSerDeConstants.CHAR_TYPE_NAME + "\"," +
            "\"maxLength\":" + ((MetastorePrimitiveTypeInfo) typeInfo).getParameters()[0] + "}");
        break;
      case ColumnType.VARCHAR_TYPE_NAME:
        schema = AvroSchemaUtils.getSchemaFor("{" +
            "\"type\":\"" + AvroSerDeConstants.AVRO_STRING_TYPE_NAME + "\"," +
            "\"logicalType\":\"" + AvroSerDeConstants.VARCHAR_TYPE_NAME + "\"," +
            "\"maxLength\":" + ((MetastorePrimitiveTypeInfo) typeInfo).getParameters()[0] + "}");
        break;
      case ColumnType.BINARY_TYPE_NAME:
        schema = Schema.create(Schema.Type.BYTES);
        break;
      case ColumnType.TINYINT_TYPE_NAME:
        schema = Schema.create(Schema.Type.INT);
        break;
      case ColumnType.SMALLINT_TYPE_NAME:
        schema = Schema.create(Schema.Type.INT);
        break;
      case ColumnType.INT_TYPE_NAME:
        schema = Schema.create(Schema.Type.INT);
        break;
      case ColumnType.BIGINT_TYPE_NAME:
        schema = Schema.create(Schema.Type.LONG);
        break;
      case ColumnType.FLOAT_TYPE_NAME:
        schema = Schema.create(Schema.Type.FLOAT);
        break;
      case ColumnType.DOUBLE_TYPE_NAME:
        schema = Schema.create(Schema.Type.DOUBLE);
        break;
      case ColumnType.BOOLEAN_TYPE_NAME:
        schema = Schema.create(Schema.Type.BOOLEAN);
        break;
      case ColumnType.DECIMAL_TYPE_NAME:
        String precision = String.valueOf(((MetastorePrimitiveTypeInfo) typeInfo).getParameters()[0]);
        String scale = String.valueOf(((MetastorePrimitiveTypeInfo) typeInfo).getParameters()[1]);
        schema = AvroSchemaUtils.getSchemaFor("{" +
            "\"type\":\"bytes\"," +
            "\"logicalType\":\"decimal\"," +
            "\"precision\":" + precision + "," +
            "\"scale\":" + scale + "}");
        break;
      case ColumnType.DATE_TYPE_NAME:
        schema = AvroSchemaUtils.getSchemaFor("{" +
            "\"type\":\"" + AvroSerDeConstants.AVRO_INT_TYPE_NAME + "\"," +
            "\"logicalType\":\"" + AvroSerDeConstants.DATE_TYPE_NAME + "\"}");
        break;
      case ColumnType.TIMESTAMP_TYPE_NAME:
        schema = AvroSchemaUtils.getSchemaFor("{" +
          "\"type\":\"" + AvroSerDeConstants.AVRO_LONG_TYPE_NAME + "\"," +
          "\"logicalType\":\"" + AvroSerDeConstants.AVRO_TIMESTAMP_TYPE_NAME + "\"}");
        break;
      case ColumnType.VOID_TYPE_NAME:
        schema = Schema.create(Schema.Type.NULL);
        break;
      default:
        throw new UnsupportedOperationException(typeInfo + " is not supported.");
    }
    return schema;
  }

  private Schema createAvroUnion(TypeInfo typeInfo) {
    List<Schema> childSchemas = new ArrayList<Schema>();
    for (TypeInfo childTypeInfo : ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos()) {
      final Schema childSchema = createAvroSchema(childTypeInfo);
      if (childSchema.getType() == Schema.Type.UNION) {
        childSchemas.addAll(childSchema.getTypes());
      } else {
        childSchemas.add(childSchema);
      }
    }

    return Schema.createUnion(removeDuplicateNullSchemas(childSchemas));
  }

  private Schema createAvroRecord(TypeInfo typeInfo) {
    List<Schema.Field> childFields = new ArrayList<Schema.Field>();
    final List<String> allStructFieldNames = ((StructTypeInfo) typeInfo).getAllStructFieldNames();
    final List<TypeInfo> allStructFieldTypeInfos =
        ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
    if (allStructFieldNames.size() != allStructFieldTypeInfos.size()) {
      throw new IllegalArgumentException("Failed to generate avro schema from hive schema. " +
          "name and column type differs. names = " + allStructFieldNames + ", types = " +
          allStructFieldTypeInfos);
    }

    for (int i = 0; i < allStructFieldNames.size(); ++i) {
      final TypeInfo childTypeInfo = allStructFieldTypeInfos.get(i);
      final Schema.Field grandChildSchemaField = createAvroField(allStructFieldNames.get(i),
          childTypeInfo, childTypeInfo.toString());
      final List<Schema.Field> grandChildFields = getFields(grandChildSchemaField);
      childFields.addAll(grandChildFields);
    }

    Schema recordSchema = Schema.createRecord("record_" + recordCounter, typeInfo.toString(),
        null, false);
    ++recordCounter;
    recordSchema.setFields(childFields);
    return recordSchema;
  }

  private Schema createAvroMap(TypeInfo typeInfo) {
    TypeInfo keyTypeInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
    if (!ColumnType.STRING_TYPE_NAME.equals(keyTypeInfo.getTypeName())) {
      throw new UnsupportedOperationException("Key of Map can only be a String");
    }

    TypeInfo valueTypeInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
    Schema valueSchema = createAvroSchema(valueTypeInfo);

    return Schema.createMap(valueSchema);
  }

  private Schema createAvroArray(TypeInfo typeInfo) {
    ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
    Schema listSchema = createAvroSchema(listTypeInfo.getListElementTypeInfo());
    return Schema.createArray(listSchema);
  }

  private List<Schema.Field> getFields(Schema.Field schemaField) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();

    JsonNode nullDefault = JsonNodeFactory.instance.nullNode();
    if (schemaField.schema().getType() == Schema.Type.RECORD) {
      for (Schema.Field field : schemaField.schema().getFields()) {
        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), nullDefault));
      }
    } else {
      fields.add(new Schema.Field(schemaField.name(), schemaField.schema(), schemaField.doc(),
          nullDefault));
    }

    return fields;
  }

  private Schema wrapInUnionWithNull(Schema schema) {
    Schema wrappedSchema = schema;
    switch (schema.getType()) {
      case NULL:
        break;
      case UNION:
        List<Schema> existingSchemas = removeDuplicateNullSchemas(schema.getTypes());
        wrappedSchema = Schema.createUnion(existingSchemas);
        break;
      default:
        wrappedSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

    return wrappedSchema;
  }

  private List<Schema> removeDuplicateNullSchemas(List<Schema> childSchemas) {
    List<Schema> prunedSchemas = new ArrayList<Schema>();
    boolean isNullPresent = false;
    for (Schema schema : childSchemas) {
      if (schema.getType() == Schema.Type.NULL) {
        isNullPresent = true;
      } else {
        prunedSchemas.add(schema);
      }
    }
    if (isNullPresent) {
      prunedSchemas.add(0, Schema.create(Schema.Type.NULL));
    }

    return prunedSchemas;
  }
}