/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.convert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.schema.*;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ParquetToHiveSchemaConverter {

  private static final String INT96_IS_TS_PROPERTY_KEY = "parquet.int96.is.timestamp";

  private Properties properties = null;

  public ParquetToHiveSchemaConverter() {}
  public ParquetToHiveSchemaConverter(Properties props) {
    properties = props;
  }

  private static final Log LOG = LogFactory.getLog(ParquetToHiveSchemaConverter.class);

  public StructTypeInfo convert(GroupType parquetSchema) {
    return convertFields(parquetSchema.getFields());
  }

  private StructTypeInfo convertFields(List<Type> parquetFields) {
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    ArrayList<String> names = new ArrayList<String>();
    ArrayList<TypeInfo> types = new ArrayList<TypeInfo>();

    for (Type parquetType : parquetFields) {

      TypeInfo type;
      if (parquetType.isRepetition(Type.Repetition.REPEATED)) {
        type = createHiveArray(parquetType, "");
      } else {
        type = convertField(parquetType);
      }

      names.add(parquetType.getName());
      types.add(type);
    }

    structTypeInfo.setAllStructFieldNames(names);
    structTypeInfo.setAllStructFieldTypeInfos(types);

    LOG.info("Generated Hive's StructTypeInfo from parquet schema is: " + structTypeInfo);

    return structTypeInfo;
  }

  private TypeInfo convertField(final Type parquetType) {
    if (parquetType.isPrimitive()) {
      final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
          parquetType.asPrimitiveType().getPrimitiveTypeName();
      final OriginalType originalType = parquetType.getOriginalType();

      if (originalType == OriginalType.DECIMAL) {
        final DecimalMetadata decimalMetadata = parquetType.asPrimitiveType()
            .getDecimalMetadata();
        return TypeInfoFactory.getDecimalTypeInfo(decimalMetadata.getPrecision(),
            decimalMetadata.getScale());
      }

      if (parquetPrimitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT96)) {
        if (properties == null || !properties.containsKey(INT96_IS_TS_PROPERTY_KEY)) {
          throw new UnsupportedOperationException("Parquet's INT96 does not have a valid mapping" +
              " to a Hive type.\nIf you want Parquet's INT96 to be mapped to Hive's timestamp," +
              " then set '" + INT96_IS_TS_PROPERTY_KEY + "' in the table properties. Otherwise, " +
              "provide hive schema explicitly in the DDL statement");
        }
      }

      return parquetPrimitiveTypeName.convert(
          new PrimitiveType.PrimitiveTypeNameConverter<TypeInfo, RuntimeException>() {
            @Override
            public TypeInfo convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.booleanTypeInfo;
            }

            @Override
            public TypeInfo convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.intTypeInfo;
            }

            @Override
            public TypeInfo convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.longTypeInfo;
            }

            @Override
            public TypeInfo convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.timestampTypeInfo;
            }

            @Override
            public TypeInfo convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.floatTypeInfo;
            }

            @Override
            public TypeInfo convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              return TypeInfoFactory.doubleTypeInfo;
            }

            @Override
            public TypeInfo convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName
                                                          primitiveTypeName) {
              return TypeInfoFactory.binaryTypeInfo;
            }

            @Override
            public TypeInfo convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
              if (originalType == OriginalType.UTF8 || originalType == OriginalType.ENUM) {
                return TypeInfoFactory.stringTypeInfo;
              } else {
                return TypeInfoFactory.binaryTypeInfo;
              }
            }
          });
    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      OriginalType originalType = parquetGroupType.getOriginalType();
      if (originalType != null) {
        switch (originalType) {
          case LIST:
            if (parquetGroupType.getFieldCount() != 1) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            Type elementType = parquetGroupType.getType(0);
            if (!elementType.isRepetition(Type.Repetition.REPEATED)) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            return createHiveArray(elementType, parquetGroupType.getName());
          case MAP:
          case MAP_KEY_VALUE:
            if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
            if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED) ||
                !mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE) ||
                mapKeyValType.getFieldCount() != 2) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            Type keyType = mapKeyValType.getType(0);
            if (!keyType.isPrimitive() ||
                !keyType.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType
                    .PrimitiveTypeName.BINARY) ||
                !keyType.getOriginalType().equals(OriginalType.UTF8)) {
              throw new UnsupportedOperationException("Map key type must be binary (UTF8): "
                  + keyType);
            }
            Type valueType = mapKeyValType.getType(1);
            return createHiveMap(convertField(keyType), convertField(valueType));
          case ENUM:
          case UTF8:
            return TypeInfoFactory.stringTypeInfo;
          default:
            throw new UnsupportedOperationException("Cannot convert Parquet type " +
                parquetType);
        }
      } else {
        // if no original type then it's a record
        return createHiveStruct(parquetGroupType.getFields());
      }
    }
  }

  private TypeInfo createHiveStruct(List<Type> parquetFields) {
    List<String> names = new ArrayList<String>();
    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();

    for (Type field: parquetFields) {
      names.add(field.getName());
      typeInfos.add(convertField(field));
    }

    return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

  private TypeInfo createHiveMap(TypeInfo keyType, TypeInfo valueType) {
    return TypeInfoFactory.getMapTypeInfo(keyType, valueType);
  }

  private TypeInfo createHiveArray(Type elementType, String elementName) {
    if (elementType.isPrimitive()) {
      return TypeInfoFactory.getListTypeInfo(convertField(elementType));
    } else {
      final GroupType groupType = elementType.asGroupType();
      final List<Type> groupFields = groupType.getFields();
      if (groupFields.size() > 1 ||
          (groupFields.size() == 1 &&
              (elementType.getName().equals("array") ||
                  elementType.getName().equals(elementName + "_tuple")))) {
        return TypeInfoFactory.getListTypeInfo(createHiveStruct(groupFields));
      } else {
        return TypeInfoFactory.getListTypeInfo(convertField(groupType.getFields().get(0)));
      }
    }
  }
}
