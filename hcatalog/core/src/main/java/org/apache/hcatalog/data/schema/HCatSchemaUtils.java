/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.data.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatContext;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;


/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.data.schema.HCatSchemaUtils} instead
 */
public class HCatSchemaUtils {

  public static CollectionBuilder getStructSchemaBuilder() {
    return new CollectionBuilder();
  }

  public static CollectionBuilder getListSchemaBuilder() {
    return new CollectionBuilder();
  }

  public static MapBuilder getMapSchemaBuilder() {
    return new MapBuilder();
  }


  public static abstract class HCatSchemaBuilder {
    public abstract HCatSchema build() throws HCatException;
  }

  public static class CollectionBuilder extends HCatSchemaBuilder { // for STRUCTS(multiple-add-calls) and LISTS(single-add-call)
    List<HCatFieldSchema> fieldSchemas = null;

    CollectionBuilder() {
      fieldSchemas = new ArrayList<HCatFieldSchema>();
    }

    public CollectionBuilder addField(FieldSchema fieldSchema) throws HCatException {
      return this.addField(getHCatFieldSchema(fieldSchema));
    }

    public CollectionBuilder addField(HCatFieldSchema fieldColumnSchema) {
      fieldSchemas.add(fieldColumnSchema);
      return this;
    }

    @Override
    public HCatSchema build() throws HCatException {
      return new HCatSchema(fieldSchemas);
    }

  }

  public static class MapBuilder extends HCatSchemaBuilder {

    Type keyType = null;
    HCatSchema valueSchema = null;

    @Override
    public HCatSchema build() throws HCatException {
      List<HCatFieldSchema> fslist = new ArrayList<HCatFieldSchema>();
      fslist.add(new HCatFieldSchema(null, Type.MAP, keyType, valueSchema, null));
      return new HCatSchema(fslist);
    }

    public MapBuilder withValueSchema(HCatSchema valueSchema) {
      this.valueSchema = valueSchema;
      return this;
    }

    public MapBuilder withKeyType(Type keyType) {
      this.keyType = keyType;
      return this;
    }

  }


  /**
   * Convert a HCatFieldSchema to a FieldSchema
   * @param fs FieldSchema to convert
   * @return HCatFieldSchema representation of FieldSchema
   * @throws HCatException
   */
  public static HCatFieldSchema getHCatFieldSchema(FieldSchema fs) throws HCatException {
    String fieldName = fs.getName();
    TypeInfo baseTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fs.getType());
    return getHCatFieldSchema(fieldName, baseTypeInfo);
  }

  private static HCatFieldSchema getHCatFieldSchema(String fieldName, TypeInfo fieldTypeInfo) throws HCatException {
    Category typeCategory = fieldTypeInfo.getCategory();
    HCatFieldSchema hCatFieldSchema;
    if (Category.PRIMITIVE == typeCategory) {
      hCatFieldSchema = new HCatFieldSchema(fieldName, getPrimitiveHType(fieldTypeInfo), null);
    } else if (Category.STRUCT == typeCategory) {
      HCatSchema subSchema = constructHCatSchema((StructTypeInfo) fieldTypeInfo);
      hCatFieldSchema = new HCatFieldSchema(fieldName, HCatFieldSchema.Type.STRUCT, subSchema, null);
    } else if (Category.LIST == typeCategory) {
      HCatSchema subSchema = getHCatSchema(((ListTypeInfo) fieldTypeInfo).getListElementTypeInfo());
      hCatFieldSchema = new HCatFieldSchema(fieldName, HCatFieldSchema.Type.ARRAY, subSchema, null);
    } else if (Category.MAP == typeCategory) {
      HCatFieldSchema.Type mapKeyType = getPrimitiveHType(((MapTypeInfo) fieldTypeInfo).getMapKeyTypeInfo());
      HCatSchema subSchema = getHCatSchema(((MapTypeInfo) fieldTypeInfo).getMapValueTypeInfo());
      hCatFieldSchema = new HCatFieldSchema(fieldName, HCatFieldSchema.Type.MAP, mapKeyType, subSchema, null);
    } else {
      throw new TypeNotPresentException(fieldTypeInfo.getTypeName(), null);
    }
    return hCatFieldSchema;
  }

  private static Type getPrimitiveHType(TypeInfo basePrimitiveTypeInfo) {
    switch (((PrimitiveTypeInfo) basePrimitiveTypeInfo).getPrimitiveCategory()) {
    case BOOLEAN:
      return (HCatContext.INSTANCE.getConf().isPresent() &&
        HCatContext.INSTANCE.getConf().get().getBoolean(
          HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER,
          HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER_DEFAULT)) ?
        Type.INT : Type.BOOLEAN;
    case BYTE:
      return (HCatContext.INSTANCE.getConf().isPresent() &&
        HCatContext.INSTANCE.getConf().get().getBoolean(
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION,
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT)) ?
        Type.INT : Type.TINYINT;
    case DOUBLE:
      return Type.DOUBLE;
    case FLOAT:
      return Type.FLOAT;
    case INT:
      return Type.INT;
    case LONG:
      return Type.BIGINT;
    case SHORT:
      return (HCatContext.INSTANCE.getConf().isPresent() &&
        HCatContext.INSTANCE.getConf().get().getBoolean(
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION,
          HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION_DEFAULT)) ?
        Type.INT : Type.SMALLINT;
    case STRING:
      return Type.STRING;
    case BINARY:
      return Type.BINARY;
    default:
      throw new TypeNotPresentException(((PrimitiveTypeInfo) basePrimitiveTypeInfo).getTypeName(), null);
    }
  }

  public static HCatSchema getHCatSchema(Schema schema) throws HCatException {
    return getHCatSchema(schema.getFieldSchemas());
  }

  public static HCatSchema getHCatSchema(List<? extends FieldSchema> fslist) throws HCatException {
    CollectionBuilder builder = getStructSchemaBuilder();
    for (FieldSchema fieldSchema : fslist) {
      builder.addField(fieldSchema);
    }
    return builder.build();
  }

  private static HCatSchema constructHCatSchema(StructTypeInfo stypeInfo) throws HCatException {
    CollectionBuilder builder = getStructSchemaBuilder();
    for (String fieldName : ((StructTypeInfo) stypeInfo).getAllStructFieldNames()) {
      builder.addField(getHCatFieldSchema(fieldName, ((StructTypeInfo) stypeInfo).getStructFieldTypeInfo(fieldName)));
    }
    return builder.build();
  }

  public static HCatSchema getHCatSchema(TypeInfo typeInfo) throws HCatException {
    Category typeCategory = typeInfo.getCategory();
    HCatSchema hCatSchema;
    if (Category.PRIMITIVE == typeCategory) {
      hCatSchema = getStructSchemaBuilder().addField(new HCatFieldSchema(null, getPrimitiveHType(typeInfo), null)).build();
    } else if (Category.STRUCT == typeCategory) {
      HCatSchema subSchema = constructHCatSchema((StructTypeInfo) typeInfo);
      hCatSchema = getStructSchemaBuilder().addField(new HCatFieldSchema(null, Type.STRUCT, subSchema, null)).build();
    } else if (Category.LIST == typeCategory) {
      CollectionBuilder builder = getListSchemaBuilder();
      builder.addField(getHCatFieldSchema(null, ((ListTypeInfo) typeInfo).getListElementTypeInfo()));
      hCatSchema = new HCatSchema(Arrays.asList(new HCatFieldSchema("", Type.ARRAY, builder.build(), "")));
    } else if (Category.MAP == typeCategory) {
      HCatFieldSchema.Type mapKeyType = getPrimitiveHType(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
      HCatSchema subSchema = getHCatSchema(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
      MapBuilder builder = getMapSchemaBuilder();
      hCatSchema = builder.withKeyType(mapKeyType).withValueSchema(subSchema).build();
    } else {
      throw new TypeNotPresentException(typeInfo.getTypeName(), null);
    }
    return hCatSchema;
  }

  public static HCatSchema getHCatSchemaFromTypeString(String typeString) throws HCatException {
    return getHCatSchema(TypeInfoUtils.getTypeInfoFromTypeString(typeString));
  }

  public static HCatSchema getHCatSchema(String schemaString) throws HCatException {
    if ((schemaString == null) || (schemaString.trim().isEmpty())) {
      return new HCatSchema(new ArrayList<HCatFieldSchema>()); // empty HSchema construct
    }
    HCatSchema outerSchema = getHCatSchemaFromTypeString("struct<" + schemaString + ">");
    return outerSchema.get(0).getStructSubSchema();
  }

  public static FieldSchema getFieldSchema(HCatFieldSchema hcatFieldSchema) {
    return new FieldSchema(hcatFieldSchema.getName(), hcatFieldSchema.getTypeString(), hcatFieldSchema.getComment());
  }

  public static List<FieldSchema> getFieldSchemas(List<HCatFieldSchema> hcatFieldSchemas) {
    List<FieldSchema> lfs = new ArrayList<FieldSchema>();
    for (HCatFieldSchema hfs : hcatFieldSchemas) {
      lfs.add(getFieldSchema(hfs));
    }
    return lfs;
  }
}
