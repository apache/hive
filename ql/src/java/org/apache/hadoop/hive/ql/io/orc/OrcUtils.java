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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import com.google.common.collect.Lists;

public class OrcUtils {
  private static final Log LOG = LogFactory.getLog(OrcUtils.class);

  /**
   * Returns selected columns as a boolean array with true value set for specified column names.
   * The result will contain number of elements equal to flattened number of columns.
   * For example:
   * selectedColumns - a,b,c
   * allColumns - a,b,c,d
   * If column c is a complex type, say list<string> and other types are primitives then result will
   * be [false, true, true, true, true, true, false]
   * Index 0 is the root element of the struct which is set to false by default, index 1,2
   * corresponds to columns a and b. Index 3,4 correspond to column c which is list<string> and
   * index 5 correspond to column d. After flattening list<string> gets 2 columns.
   *
   * @param selectedColumns - comma separated list of selected column names
   * @param allColumns      - comma separated list of all column names
   * @param inspector       - object inspector
   * @return - boolean array with true value set for the specified column names
   */
  public static boolean[] includeColumns(String selectedColumns, String allColumns,
      ObjectInspector inspector) {
    int numFlattenedCols = getFlattenedColumnsCount(inspector);
    boolean[] results = new boolean[numFlattenedCols];
    if ("*".equals(selectedColumns)) {
      Arrays.fill(results, true);
      return results;
    }
    if (selectedColumns != null && !selectedColumns.isEmpty()) {
      includeColumnsImpl(results, selectedColumns.toLowerCase(), allColumns, inspector);
    }
    return results;
  }

  private static void includeColumnsImpl(boolean[] includeColumns, String selectedColumns,
      String allColumns,
      ObjectInspector inspector) {
      Map<String, List<Integer>> columnSpanMap = getColumnSpan(allColumns, inspector);
      LOG.info("columnSpanMap: " + columnSpanMap);

      String[] selCols = selectedColumns.split(",");
      for (String sc : selCols) {
        if (columnSpanMap.containsKey(sc)) {
          List<Integer> colSpan = columnSpanMap.get(sc);
          int start = colSpan.get(0);
          int end = colSpan.get(1);
          for (int i = start; i <= end; i++) {
            includeColumns[i] = true;
          }
        }
      }

      LOG.info("includeColumns: " + Arrays.toString(includeColumns));
    }

  private static Map<String, List<Integer>> getColumnSpan(String allColumns,
      ObjectInspector inspector) {
    // map that contains the column span for each column. Column span is the number of columns
    // required after flattening. For a given object inspector this map contains the start column
    // id and end column id (both inclusive) after flattening.
    // EXAMPLE:
    // schema: struct<a:int, b:float, c:map<string,int>>
    // column span map for the above struct will be
    // a => [1,1], b => [2,2], c => [3,5]
    Map<String, List<Integer>> columnSpanMap = new HashMap<String, List<Integer>>();
    if (allColumns != null) {
      String[] columns = allColumns.split(",");
      int startIdx = 0;
      int endIdx = 0;
      if (inspector instanceof StructObjectInspector) {
        StructObjectInspector soi = (StructObjectInspector) inspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          StructField sf = fields.get(i);

          // we get the type (category) from object inspector but column name from the argument.
          // The reason for this is hive (FileSinkOperator) does not pass the actual column names,
          // instead it passes the internal column names (_col1,_col2).
          ObjectInspector sfOI = sf.getFieldObjectInspector();
          String colName = columns[i];

          startIdx = endIdx + 1;
          switch (sfOI.getCategory()) {
            case PRIMITIVE:
              endIdx += 1;
              break;
            case STRUCT:
              endIdx += 1;
              StructObjectInspector structInsp = (StructObjectInspector) sfOI;
              List<? extends StructField> structFields = structInsp.getAllStructFieldRefs();
              for (int j = 0; j < structFields.size(); ++j) {
                endIdx += getFlattenedColumnsCount(structFields.get(j).getFieldObjectInspector());
              }
              break;
            case MAP:
              endIdx += 1;
              MapObjectInspector mapInsp = (MapObjectInspector) sfOI;
              endIdx += getFlattenedColumnsCount(mapInsp.getMapKeyObjectInspector());
              endIdx += getFlattenedColumnsCount(mapInsp.getMapValueObjectInspector());
              break;
            case LIST:
              endIdx += 1;
              ListObjectInspector listInsp = (ListObjectInspector) sfOI;
              endIdx += getFlattenedColumnsCount(listInsp.getListElementObjectInspector());
              break;
            case UNION:
              endIdx += 1;
              UnionObjectInspector unionInsp = (UnionObjectInspector) sfOI;
              List<ObjectInspector> choices = unionInsp.getObjectInspectors();
              for (int j = 0; j < choices.size(); ++j) {
                endIdx += getFlattenedColumnsCount(choices.get(j));
              }
              break;
            default:
              throw new IllegalArgumentException("Bad category: " +
                  inspector.getCategory());
          }

          columnSpanMap.put(colName, Lists.newArrayList(startIdx, endIdx));
        }
      }
    }
    return columnSpanMap;
  }

  /**
   * Returns the number of columns after flatting complex types.
   *
   * @param inspector - object inspector
   * @return
   */
  public static int getFlattenedColumnsCount(ObjectInspector inspector) {
    int numWriters = 0;
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        numWriters += 1;
        break;
      case STRUCT:
        numWriters += 1;
        StructObjectInspector structInsp = (StructObjectInspector) inspector;
        List<? extends StructField> fields = structInsp.getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); ++i) {
          numWriters += getFlattenedColumnsCount(fields.get(i).getFieldObjectInspector());
        }
        break;
      case MAP:
        numWriters += 1;
        MapObjectInspector mapInsp = (MapObjectInspector) inspector;
        numWriters += getFlattenedColumnsCount(mapInsp.getMapKeyObjectInspector());
        numWriters += getFlattenedColumnsCount(mapInsp.getMapValueObjectInspector());
        break;
      case LIST:
        numWriters += 1;
        ListObjectInspector listInsp = (ListObjectInspector) inspector;
        numWriters += getFlattenedColumnsCount(listInsp.getListElementObjectInspector());
        break;
      case UNION:
        numWriters += 1;
        UnionObjectInspector unionInsp = (UnionObjectInspector) inspector;
        List<ObjectInspector> choices = unionInsp.getObjectInspectors();
        for (int i = 0; i < choices.size(); ++i) {
          numWriters += getFlattenedColumnsCount(choices.get(i));
        }
        break;
      default:
        throw new IllegalArgumentException("Bad category: " +
            inspector.getCategory());
    }
    return numWriters;
  }

  /**
   * Convert a Hive type property string that contains separated type names into a list of
   * TypeDescription objects.
   * @return the list of TypeDescription objects.
   */
  public static ArrayList<TypeDescription> typeDescriptionsFromHiveTypeProperty(
      String hiveTypeProperty) {

    // CONSDIER: We need a type name parser for TypeDescription.

    ArrayList<TypeInfo> typeInfoList = TypeInfoUtils.getTypeInfosFromTypeString(hiveTypeProperty);
    ArrayList<TypeDescription> typeDescrList =new ArrayList<TypeDescription>(typeInfoList.size());
    for (TypeInfo typeInfo : typeInfoList) {
      typeDescrList.add(convertTypeInfo(typeInfo));
    }
    return typeDescrList;
  }

  public static TypeDescription convertTypeInfo(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo pinfo = (PrimitiveTypeInfo) info;
        switch (pinfo.getPrimitiveCategory()) {
          case BOOLEAN:
            return TypeDescription.createBoolean();
          case BYTE:
            return TypeDescription.createByte();
          case SHORT:
            return TypeDescription.createShort();
          case INT:
            return TypeDescription.createInt();
          case LONG:
            return TypeDescription.createLong();
          case FLOAT:
            return TypeDescription.createFloat();
          case DOUBLE:
            return TypeDescription.createDouble();
          case STRING:
            return TypeDescription.createString();
          case DATE:
            return TypeDescription.createDate();
          case TIMESTAMP:
            return TypeDescription.createTimestamp();
          case BINARY:
            return TypeDescription.createBinary();
          case DECIMAL: {
            DecimalTypeInfo dinfo = (DecimalTypeInfo) pinfo;
            return TypeDescription.createDecimal()
                .withScale(dinfo.getScale())
                .withPrecision(dinfo.getPrecision());
          }
          case VARCHAR: {
            BaseCharTypeInfo cinfo = (BaseCharTypeInfo) pinfo;
            return TypeDescription.createVarchar()
                .withMaxLength(cinfo.getLength());
          }
          case CHAR: {
            BaseCharTypeInfo cinfo = (BaseCharTypeInfo) pinfo;
            return TypeDescription.createChar()
                .withMaxLength(cinfo.getLength());
          }
          default:
            throw new IllegalArgumentException("ORC doesn't handle primitive" +
                " category " + pinfo.getPrimitiveCategory());
        }
      }
      case LIST: {
        ListTypeInfo linfo = (ListTypeInfo) info;
        return TypeDescription.createList
            (convertTypeInfo(linfo.getListElementTypeInfo()));
      }
      case MAP: {
        MapTypeInfo minfo = (MapTypeInfo) info;
        return TypeDescription.createMap
            (convertTypeInfo(minfo.getMapKeyTypeInfo()),
                convertTypeInfo(minfo.getMapValueTypeInfo()));
      }
      case UNION: {
        UnionTypeInfo minfo = (UnionTypeInfo) info;
        TypeDescription result = TypeDescription.createUnion();
        for (TypeInfo child: minfo.getAllUnionObjectTypeInfos()) {
          result.addUnionChild(convertTypeInfo(child));
        }
        return result;
      }
      case STRUCT: {
        StructTypeInfo sinfo = (StructTypeInfo) info;
        TypeDescription result = TypeDescription.createStruct();
        for(String fieldName: sinfo.getAllStructFieldNames()) {
          result.addField(fieldName,
              convertTypeInfo(sinfo.getStructFieldTypeInfo(fieldName)));
        }
        return result;
      }
      default:
        throw new IllegalArgumentException("ORC doesn't handle " +
            info.getCategory());
    }
  }

  public static List<OrcProto.Type> getOrcTypes(TypeDescription typeDescr) {
    List<OrcProto.Type> result = Lists.newArrayList();
    appendOrcTypes(result, typeDescr);
    return result;
  }

  private static void appendOrcTypes(List<OrcProto.Type> result, TypeDescription typeDescr) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    List<TypeDescription> children = typeDescr.getChildren();
    switch (typeDescr.getCategory()) {
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
      type.setKind(OrcProto.Type.Kind.CHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case VARCHAR:
      type.setKind(Type.Kind.VARCHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
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
      type.setKind(OrcProto.Type.Kind.DECIMAL);
      type.setPrecision(typeDescr.getPrecision());
      type.setScale(typeDescr.getScale());
      break;
    case LIST:
      type.setKind(OrcProto.Type.Kind.LIST);
      type.addSubtypes(children.get(0).getId());
      break;
    case MAP:
      type.setKind(OrcProto.Type.Kind.MAP);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      break;
    case STRUCT:
      type.setKind(OrcProto.Type.Kind.STRUCT);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      for(String field: typeDescr.getFieldNames()) {
        type.addFieldNames(field);
      }
      break;
    case UNION:
      type.setKind(OrcProto.Type.Kind.UNION);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " +
          typeDescr.getCategory());
    }
    result.add(type.build());
    if (children != null) {
      for(TypeDescription child: children) {
        appendOrcTypes(result, child);
      }
    }
  }

  /**
   * NOTE: This method ignores the subtype numbers in the TypeDescription rebuilds the subtype
   * numbers based on the length of the result list being appended.
   *
   * @param result
   * @param typeInfo
   */
  public static void appendOrcTypesRebuildSubtypes(List<OrcProto.Type> result,
      TypeDescription typeDescr) {

    int subtype = result.size();
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    boolean needsAdd = true;
    List<TypeDescription> children = typeDescr.getChildren();
    switch (typeDescr.getCategory()) {
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
      type.setKind(OrcProto.Type.Kind.CHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case VARCHAR:
      type.setKind(Type.Kind.VARCHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
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
      type.setKind(OrcProto.Type.Kind.DECIMAL);
      type.setPrecision(typeDescr.getPrecision());
      type.setScale(typeDescr.getScale());
      break;
    case LIST:
      type.setKind(OrcProto.Type.Kind.LIST);
      type.addSubtypes(++subtype);
      result.add(type.build());
      needsAdd = false;
      appendOrcTypesRebuildSubtypes(result, children.get(0));
      break;
    case MAP:
      {
        // Make room for MAP type.
        result.add(null);
  
        // Add MAP type pair in order to determine their subtype values.
        appendOrcTypesRebuildSubtypes(result, children.get(0));
        int subtype2 = result.size();
        appendOrcTypesRebuildSubtypes(result, children.get(1));
        type.setKind(OrcProto.Type.Kind.MAP);
        type.addSubtypes(subtype + 1);
        type.addSubtypes(subtype2);
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    case STRUCT:
      {
        List<String> fieldNames = typeDescr.getFieldNames();

        // Make room for STRUCT type.
        result.add(null);

        List<Integer> fieldSubtypes = new ArrayList<Integer>(fieldNames.size());
        for(TypeDescription child: children) {
          int fieldSubtype = result.size();
          fieldSubtypes.add(fieldSubtype);
          appendOrcTypesRebuildSubtypes(result, child);
        }

        type.setKind(OrcProto.Type.Kind.STRUCT);

        for (int i = 0 ; i < fieldNames.size(); i++) {
          type.addSubtypes(fieldSubtypes.get(i));
          type.addFieldNames(fieldNames.get(i));
        }
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    case UNION:
      {
        // Make room for UNION type.
        result.add(null);

        List<Integer> unionSubtypes = new ArrayList<Integer>(children.size());
        for(TypeDescription child: children) {
          int unionSubtype = result.size();
          unionSubtypes.add(unionSubtype);
          appendOrcTypesRebuildSubtypes(result, child);
        }

        type.setKind(OrcProto.Type.Kind.UNION);
        for (int i = 0 ; i < children.size(); i++) {
          type.addSubtypes(unionSubtypes.get(i));
        }
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " + typeDescr.getCategory());
    }
    if (needsAdd) {
      result.add(type.build());
    }
  }

  /**
   * NOTE: This method ignores the subtype numbers in the OrcProto.Type rebuilds the subtype
   * numbers based on the length of the result list being appended.
   *
   * @param result
   * @param typeInfo
   */
  public static int appendOrcTypesRebuildSubtypes(List<OrcProto.Type> result,
      List<OrcProto.Type> types, int columnId) {

    OrcProto.Type oldType = types.get(columnId++);

    int subtype = result.size();
    OrcProto.Type.Builder builder = OrcProto.Type.newBuilder();
    boolean needsAdd = true;
    switch (oldType.getKind()) {
    case BOOLEAN:
      builder.setKind(OrcProto.Type.Kind.BOOLEAN);
      break;
    case BYTE:
      builder.setKind(OrcProto.Type.Kind.BYTE);
      break;
    case SHORT:
      builder.setKind(OrcProto.Type.Kind.SHORT);
      break;
    case INT:
      builder.setKind(OrcProto.Type.Kind.INT);
      break;
    case LONG:
      builder.setKind(OrcProto.Type.Kind.LONG);
      break;
    case FLOAT:
      builder.setKind(OrcProto.Type.Kind.FLOAT);
      break;
    case DOUBLE:
      builder.setKind(OrcProto.Type.Kind.DOUBLE);
      break;
    case STRING:
      builder.setKind(OrcProto.Type.Kind.STRING);
      break;
    case CHAR:
      builder.setKind(OrcProto.Type.Kind.CHAR);
      builder.setMaximumLength(oldType.getMaximumLength());
      break;
    case VARCHAR:
      builder.setKind(Type.Kind.VARCHAR);
      builder.setMaximumLength(oldType.getMaximumLength());
      break;
    case BINARY:
      builder.setKind(OrcProto.Type.Kind.BINARY);
      break;
    case TIMESTAMP:
      builder.setKind(OrcProto.Type.Kind.TIMESTAMP);
      break;
    case DATE:
      builder.setKind(OrcProto.Type.Kind.DATE);
      break;
    case DECIMAL:
      builder.setKind(OrcProto.Type.Kind.DECIMAL);
      builder.setPrecision(oldType.getPrecision());
      builder.setScale(oldType.getScale());
      break;
    case LIST:
      builder.setKind(OrcProto.Type.Kind.LIST);
      builder.addSubtypes(++subtype);
      result.add(builder.build());
      needsAdd = false;
      columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
      break;
    case MAP:
      {
        // Make room for MAP type.
        result.add(null);
  
        // Add MAP type pair in order to determine their subtype values.
        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        int subtype2 = result.size();
        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        builder.setKind(OrcProto.Type.Kind.MAP);
        builder.addSubtypes(subtype + 1);
        builder.addSubtypes(subtype2);
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    case STRUCT:
      {
        List<String> fieldNames = oldType.getFieldNamesList();

        // Make room for STRUCT type.
        result.add(null);

        List<Integer> fieldSubtypes = new ArrayList<Integer>(fieldNames.size());
        for(int i = 0 ; i < fieldNames.size(); i++) {
          int fieldSubtype = result.size();
          fieldSubtypes.add(fieldSubtype);
          columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        }

        builder.setKind(OrcProto.Type.Kind.STRUCT);

        for (int i = 0 ; i < fieldNames.size(); i++) {
          builder.addSubtypes(fieldSubtypes.get(i));
          builder.addFieldNames(fieldNames.get(i));
        }
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    case UNION:
      {
        int subtypeCount = oldType.getSubtypesCount();

        // Make room for UNION type.
        result.add(null);

        List<Integer> unionSubtypes = new ArrayList<Integer>(subtypeCount);
        for(int i = 0 ; i < subtypeCount; i++) {
          int unionSubtype = result.size();
          unionSubtypes.add(unionSubtype);
          columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        }

        builder.setKind(OrcProto.Type.Kind.UNION);
        for (int i = 0 ; i < subtypeCount; i++) {
          builder.addSubtypes(unionSubtypes.get(i));
        }
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " + oldType.getKind());
    }
    if (needsAdd) {
      result.add(builder.build());
    }
    return columnId;
  }

  public static TypeDescription getDesiredRowTypeDescr(Configuration conf, boolean isAcid) {

    String columnNameProperty = null;
    String columnTypeProperty = null;

    ArrayList<String> schemaEvolutionColumnNames = null;
    ArrayList<TypeDescription> schemaEvolutionTypeDescrs = null;

    boolean haveSchemaEvolutionProperties = false;
    if (isAcid || HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION)) {

      columnNameProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
      columnTypeProperty = conf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS_TYPES);

      haveSchemaEvolutionProperties =
          (columnNameProperty != null && columnTypeProperty != null);

      if (haveSchemaEvolutionProperties) {
        schemaEvolutionColumnNames = Lists.newArrayList(columnNameProperty.split(","));
        if (schemaEvolutionColumnNames.size() == 0) {
          haveSchemaEvolutionProperties = false;
        } else {
          schemaEvolutionTypeDescrs =
              OrcUtils.typeDescriptionsFromHiveTypeProperty(columnTypeProperty);
          if (schemaEvolutionTypeDescrs.size() != schemaEvolutionColumnNames.size()) {
            haveSchemaEvolutionProperties = false;
          }
        }
      }
    }

    if (haveSchemaEvolutionProperties) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Using schema evolution configuration variables schema.evolution.columns " +
            schemaEvolutionColumnNames.toString() +
            " / schema.evolution.columns.types " +
            schemaEvolutionTypeDescrs.toString() +
            " (isAcid " + isAcid + ")");
      }
    } else {

      // Try regular properties;
      columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
      columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
      if (columnTypeProperty == null || columnNameProperty == null) {
        return null;
      }

      schemaEvolutionColumnNames = Lists.newArrayList(columnNameProperty.split(","));
      if (schemaEvolutionColumnNames.size() == 0) {
        return null;
      }
      schemaEvolutionTypeDescrs =
          OrcUtils.typeDescriptionsFromHiveTypeProperty(columnTypeProperty);
      if (schemaEvolutionTypeDescrs.size() != schemaEvolutionColumnNames.size()) {
        return null;
      }

      // Find first virtual column and clip them off.
      int virtualColumnClipNum = -1;
      int columnNum = 0;
      for (String columnName : schemaEvolutionColumnNames) {
        if (VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(columnName)) {
          virtualColumnClipNum = columnNum;
          break;
        }
        columnNum++;
      }
      if (virtualColumnClipNum != -1) {
        schemaEvolutionColumnNames =
            Lists.newArrayList(schemaEvolutionColumnNames.subList(0, virtualColumnClipNum));
        schemaEvolutionTypeDescrs = Lists.newArrayList(schemaEvolutionTypeDescrs.subList(0, virtualColumnClipNum));
      }

      if (LOG.isInfoEnabled()) {
        LOG.info("Using column configuration variables columns " +
                schemaEvolutionColumnNames.toString() +
                " / columns.types " +
                schemaEvolutionTypeDescrs.toString() +
                " (isAcid " + isAcid + ")");
      }
    }

    // Desired schema does not include virtual columns or partition columns.
    TypeDescription result = TypeDescription.createStruct();
    for (int i = 0; i < schemaEvolutionColumnNames.size(); i++) {
      result.addField(schemaEvolutionColumnNames.get(i), schemaEvolutionTypeDescrs.get(i));
    }

    return result;
  }
}
