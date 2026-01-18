/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Package private class for converting Hive schema to Iceberg schema. Should be used only by the HiveSchemaUtil.
 * Use {@link HiveSchemaUtil} for conversion purposes.
 */
class HiveSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaConverter.class);

  private int id;
  private final boolean autoConvert;

  private HiveSchemaConverter(boolean autoConvert) {
    this.autoConvert = autoConvert;
    // Iceberg starts field id assignment from 1.
    this.id = 1;
  }

  static Schema convert(List<String> names, List<TypeInfo> typeInfos, List<String> comments, boolean autoConvert,
      Map<String, String> defaultValues) {
    HiveSchemaConverter converter = new HiveSchemaConverter(autoConvert);
    return new Schema(converter.convertInternal(names, typeInfos, defaultValues, comments));
  }

  public static Type convert(TypeInfo typeInfo, boolean autoConvert, String defaultValue) {
    HiveSchemaConverter converter = new HiveSchemaConverter(autoConvert);
    return converter.convertType(typeInfo, defaultValue);
  }

  List<Types.NestedField> convertInternal(List<String> names, List<TypeInfo> typeInfos,
      Map<String, String> defaultValues, List<String> comments) {
    List<Types.NestedField> result = Lists.newArrayListWithExpectedSize(names.size());
    int outerId = id + names.size();
    id = outerId;
    for (int i = 0; i < names.size(); ++i) {
      Type type = convertType(typeInfos.get(i), defaultValues.get(names.get(i)));
      String columnName = names.get(i);
      Types.NestedField.Builder fieldBuilder =
          Types.NestedField.builder()
              .asOptional()
              .withId(outerId - names.size() + i)
              .withName(columnName)
              .ofType(type)
              .withDoc(comments.isEmpty() || i >= comments.size() ? null : comments.get(i));

      if (defaultValues.containsKey(columnName)) {
        if (type.isPrimitiveType()) {
          Object icebergDefaultValue = HiveSchemaUtil.getDefaultValue(defaultValues.get(columnName), type);
          fieldBuilder.withWriteDefault(Expressions.lit(icebergDefaultValue));
        } else if (!type.isStructType()) {
          throw new UnsupportedOperationException(
              "Default values for " + columnName + " of type " + type + " are not supported");
        }
      }

      result.add(fieldBuilder.build());
    }
    return result;
  }

  Type convertType(TypeInfo typeInfo, String defaultValue) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
          case FLOAT:
            return Types.FloatType.get();
          case DOUBLE:
            return Types.DoubleType.get();
          case BOOLEAN:
            return Types.BooleanType.get();
          case BYTE:
          case SHORT:
            Preconditions.checkArgument(autoConvert, "Unsupported Hive type %s, use integer " +
                    "instead or enable automatic type conversion, set 'iceberg.mr.schema.auto.conversion' to true",
                typeInfo.toString().toUpperCase());

            LOG.debug("Using auto conversion from SHORT/BYTE to INTEGER");
            return Types.IntegerType.get();
          case INT:
            return Types.IntegerType.get();
          case LONG:
            return Types.LongType.get();
          case BINARY:
            return Types.BinaryType.get();
          case CHAR:
          case VARCHAR:
            Preconditions.checkArgument(autoConvert, "Unsupported Hive type %s, use string " +
                    "instead or enable automatic type conversion, set 'iceberg.mr.schema.auto.conversion' to true",
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());

            LOG.debug("Using auto conversion from CHAR/VARCHAR to STRING");
            return Types.StringType.get();
          case STRING:
            return Types.StringType.get();
          case TIMESTAMP:
            TimestampTypeInfo ts = (TimestampTypeInfo) typeInfo;
            if (ts.getPrecision() == 9) {
              return Types.TimestampNanoType.withoutZone();
            }
            return Types.TimestampType.withoutZone();
          case DATE:
            return Types.DateType.get();
          case DECIMAL:
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            return Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_DAY_TIME:
          default:
            // special case for Timestamp with Local TZ which is only available in Hive3
            if ("TIMESTAMPLOCALTZ".equalsIgnoreCase(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory().name())) {
              TimestampLocalTZTypeInfo tz = (TimestampLocalTZTypeInfo) typeInfo;
              if (tz.getPrecision() == 9) {
                return Types.TimestampNanoType.withZone();
              }
              return Types.TimestampType.withZone();
            }
            throw new IllegalArgumentException("Unsupported Hive type (" +
                ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() +
                ") for Iceberg tables.");
        }
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<Types.NestedField> fields =
            convertInternal(structTypeInfo.getAllStructFieldNames(), structTypeInfo.getAllStructFieldTypeInfos(),
                HiveSchemaUtil.getDefaultValuesMap(defaultValue), Collections.emptyList());
        return Types.StructType.of(fields);
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        int keyId = id++;
        Type keyType = convertType(mapTypeInfo.getMapKeyTypeInfo(), defaultValue);
        int valueId = id++;
        Type valueType = convertType(mapTypeInfo.getMapValueTypeInfo(), defaultValue);
        return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        int listId = id++;
        Type listType = convertType(listTypeInfo.getListElementTypeInfo(), defaultValue);
        return Types.ListType.ofOptional(listId, listType);
      case VARIANT:
        return Types.VariantType.get();
      default:
        throw new IllegalArgumentException("Unknown type " + typeInfo.getCategory());
    }
  }
}
