/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import com.google.common.collect.Lists;

/**
 * Factory to create ORC tree readers. It also compares file schema with schema specified on read
 * to see if type promotions are possible.
 */
public class RecordReaderFactory {
  static final Log LOG = LogFactory.getLog(RecordReaderFactory.class);
  private static final boolean isLogInfoEnabled = LOG.isInfoEnabled();

  public static TreeReaderFactory.TreeReader createTreeReader(int colId,
      Configuration conf,
      List<OrcProto.Type> fileSchema,
      boolean[] included,
      boolean skipCorrupt) throws IOException {
    final boolean isAcid = checkAcidSchema(fileSchema);
    final List<OrcProto.Type> originalFileSchema;
    if (isAcid) {
      originalFileSchema = fileSchema.subList(fileSchema.get(0).getSubtypesCount(),
          fileSchema.size());
    } else {
      originalFileSchema = fileSchema;
    }
    final int numCols = originalFileSchema.get(0).getSubtypesCount();
    List<OrcProto.Type> schemaOnRead = getSchemaOnRead(numCols, conf);
    List<OrcProto.Type> schemaUsed = getMatchingSchema(fileSchema, schemaOnRead);
    if (schemaUsed == null) {
      return TreeReaderFactory.createTreeReader(colId, fileSchema, included, skipCorrupt);
    } else {
      return ConversionTreeReaderFactory.createTreeReader(colId, schemaUsed, included, skipCorrupt);
    }
  }

  private static boolean checkAcidSchema(List<OrcProto.Type> fileSchema) {
    if (fileSchema.get(0).getKind().equals(OrcProto.Type.Kind.STRUCT)) {
      List<String> acidFields = OrcRecordUpdater.getAcidEventFields();
      List<String> rootFields = fileSchema.get(0).getFieldNamesList();
      if (acidFields.equals(rootFields)) {
        return true;
      }
    }
    return false;
  }

  private static List<OrcProto.Type> getMatchingSchema(List<OrcProto.Type> fileSchema,
      List<OrcProto.Type> schemaOnRead) {
    if (schemaOnRead == null) {
      if (isLogInfoEnabled) {
        LOG.info("Schema is not specified on read. Using file schema.");
      }
      return null;
    }

    if (fileSchema.size() != schemaOnRead.size()) {
      if (isLogInfoEnabled) {
        LOG.info("Schema on read column count does not match file schema's column count." +
            " Falling back to using file schema.");
      }
      return null;
    } else {
      List<OrcProto.Type> result = Lists.newArrayList(fileSchema);
      // check type promotion. ORC can only support type promotions for integer types
      // short -> int -> bigint as same integer readers are used for the above types.
      boolean canPromoteType = false;
      for (int i = 0; i < fileSchema.size(); i++) {
        OrcProto.Type fColType = fileSchema.get(i);
        OrcProto.Type rColType = schemaOnRead.get(i);
        if (!fColType.getKind().equals(rColType.getKind())) {

          if (fColType.getKind().equals(OrcProto.Type.Kind.SHORT)) {

            if (rColType.getKind().equals(OrcProto.Type.Kind.INT) ||
                rColType.getKind().equals(OrcProto.Type.Kind.LONG)) {
              // type promotion possible, converting SHORT to INT/LONG requested type
              result.set(i, result.get(i).toBuilder().setKind(rColType.getKind()).build());
              canPromoteType = true;
            } else {
              canPromoteType = false;
            }

          } else if (fColType.getKind().equals(OrcProto.Type.Kind.INT)) {

            if (rColType.getKind().equals(OrcProto.Type.Kind.LONG)) {
              // type promotion possible, converting INT to LONG requested type
              result.set(i, result.get(i).toBuilder().setKind(rColType.getKind()).build());
              canPromoteType = true;
            } else {
              canPromoteType = false;
            }

          } else {
            canPromoteType = false;
          }
        }
      }

      if (canPromoteType) {
        if (isLogInfoEnabled) {
          LOG.info("Integer type promotion happened in ORC record reader. Using promoted schema.");
        }
        return result;
      }
    }

    return null;
  }

  private static List<OrcProto.Type> getSchemaOnRead(int numCols, Configuration conf) {
    String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
    if (columnTypeProperty == null || columnNameProperty == null) {
      return null;
    }

    ArrayList<String> columnNames = Lists.newArrayList(columnNameProperty.split(","));
    ArrayList<TypeInfo> fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    StructTypeInfo structTypeInfo = new StructTypeInfo();
    // Column types from conf includes virtual and partition columns at the end. We consider only
    // the actual columns in the file.
    structTypeInfo.setAllStructFieldNames(Lists.newArrayList(columnNames.subList(0, numCols)));
    structTypeInfo.setAllStructFieldTypeInfos(Lists.newArrayList(fieldTypes.subList(0, numCols)));
    ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(structTypeInfo);
    return getOrcTypes(oi);
  }

  private static List<OrcProto.Type> getOrcTypes(ObjectInspector inspector) {
    List<OrcProto.Type> result = Lists.newArrayList();
    getOrcTypesImpl(result, inspector);
    return result;
  }

  private static void getOrcTypesImpl(List<OrcProto.Type> result, ObjectInspector inspector) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case BOOLEAN:
            type.setKind(OrcProto.Type.Kind.BOOLEAN);
            break;
          case BYTE:
            type.setKind(OrcProto.Type.Kind.BYTE);
            break;
          case SHORT:
            type.setKind(OrcProto.Type.Kind.SHORT);
            break;
          case INT:
            type.setKind(OrcProto.Type.Kind.INT);
            break;
          case LONG:
            type.setKind(OrcProto.Type.Kind.LONG);
            break;
          case FLOAT:
            type.setKind(OrcProto.Type.Kind.FLOAT);
            break;
          case DOUBLE:
            type.setKind(OrcProto.Type.Kind.DOUBLE);
            break;
          case STRING:
            type.setKind(OrcProto.Type.Kind.STRING);
            break;
          case CHAR:
            // The char length needs to be written to file and should be available
            // from the object inspector
            CharTypeInfo charTypeInfo = (CharTypeInfo) ((PrimitiveObjectInspector) inspector)
                .getTypeInfo();
            type.setKind(OrcProto.Type.Kind.CHAR);
            type.setMaximumLength(charTypeInfo.getLength());
            break;
          case VARCHAR:
            // The varchar length needs to be written to file and should be available
            // from the object inspector
            VarcharTypeInfo typeInfo = (VarcharTypeInfo) ((PrimitiveObjectInspector) inspector)
                .getTypeInfo();
            type.setKind(OrcProto.Type.Kind.VARCHAR);
            type.setMaximumLength(typeInfo.getLength());
            break;
          case BINARY:
            type.setKind(OrcProto.Type.Kind.BINARY);
            break;
          case TIMESTAMP:
            type.setKind(OrcProto.Type.Kind.TIMESTAMP);
            break;
          case DATE:
            type.setKind(OrcProto.Type.Kind.DATE);
            break;
          case DECIMAL:
            DecimalTypeInfo decTypeInfo = (DecimalTypeInfo) ((PrimitiveObjectInspector) inspector)
                .getTypeInfo();
            type.setKind(OrcProto.Type.Kind.DECIMAL);
            type.setPrecision(decTypeInfo.precision());
            type.setScale(decTypeInfo.scale());
            break;
          default:
            throw new IllegalArgumentException("Unknown primitive category: " +
                ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
        }
        result.add(type.build());
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.LIST);
        result.add(type.build());
        getOrcTypesImpl(result, ((ListObjectInspector) inspector).getListElementObjectInspector());
        break;
      case MAP:
        type.setKind(OrcProto.Type.Kind.MAP);
        result.add(type.build());
        getOrcTypesImpl(result, ((MapObjectInspector) inspector).getMapKeyObjectInspector());
        getOrcTypesImpl(result, ((MapObjectInspector) inspector).getMapValueObjectInspector());
        break;
      case STRUCT:
        type.setKind(OrcProto.Type.Kind.STRUCT);
        result.add(type.build());
        for (StructField field : ((StructObjectInspector) inspector).getAllStructFieldRefs()) {
          getOrcTypesImpl(result, field.getFieldObjectInspector());
        }
        break;
      case UNION:
        type.setKind(OrcProto.Type.Kind.UNION);
        result.add(type.build());
        for (ObjectInspector oi : ((UnionObjectInspector) inspector).getObjectInspectors()) {
          getOrcTypesImpl(result, oi);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " + inspector.getCategory());
    }
  }
}
