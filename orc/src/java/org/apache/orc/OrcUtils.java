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
package org.apache.orc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

public class OrcUtils {

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
   * @param schema       - object schema
   * @return - boolean array with true value set for the specified column names
   */
  public static boolean[] includeColumns(String selectedColumns,
                                         TypeDescription schema) {
    int numFlattenedCols = schema.getMaximumId();
    boolean[] results = new boolean[numFlattenedCols + 1];
    if ("*".equals(selectedColumns)) {
      Arrays.fill(results, true);
      return results;
    }
    if (selectedColumns != null &&
        schema.getCategory() == TypeDescription.Category.STRUCT) {
      List<String> fieldNames = schema.getFieldNames();
      List<TypeDescription> fields = schema.getChildren();
      for (String column: selectedColumns.split((","))) {
        TypeDescription col = findColumn(column, fieldNames, fields);
        if (col != null) {
          for(int i=col.getId(); i <= col.getMaximumId(); ++i) {
            results[i] = true;
          }
        }
      }
    }
    return results;
  }

  private static TypeDescription findColumn(String columnName,
                                            List<String> fieldNames,
                                            List<TypeDescription> fields) {
    int i = 0;
    for(String fieldName: fieldNames) {
      if (fieldName.equalsIgnoreCase(columnName)) {
        return fields.get(i);
      } else {
        i += 1;
      }
    }
    return null;
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
      type.setKind(OrcProto.Type.Kind.VARCHAR);
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
   * @param typeDescr
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
      type.setKind(OrcProto.Type.Kind.VARCHAR);
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
   * @param types
   * @param columnId
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
      builder.setKind(OrcProto.Type.Kind.VARCHAR);
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

}
