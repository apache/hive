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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.TreeReaderFactory.TreeReaderSchema;

/**
 * Take the file types and the (optional) configuration column names/types and see if there
 * has been schema evolution.
 */
public class SchemaEvolution {

  private static final Log LOG = LogFactory.getLog(SchemaEvolution.class);

  public static TreeReaderSchema validateAndCreate(List<OrcProto.Type> fileTypes,
      List<OrcProto.Type> schemaTypes) throws IOException {

    // For ACID, the row is the ROW field in the outer STRUCT.
    final boolean isAcid = checkAcidSchema(fileTypes);
    final List<OrcProto.Type> rowSchema;
    int rowSubtype;
    if (isAcid) {
      rowSubtype = OrcRecordUpdater.ROW + 1;
      rowSchema = fileTypes.subList(rowSubtype, fileTypes.size());
    } else {
      rowSubtype = 0;
      rowSchema = fileTypes;
    }

    // Do checking on the overlap.  Additional columns will be defaulted to NULL.

    int numFileColumns = rowSchema.get(0).getSubtypesCount();
    int numDesiredColumns = schemaTypes.get(0).getSubtypesCount();

    int numReadColumns = Math.min(numFileColumns, numDesiredColumns);

    /**
     * Check type promotion.
     *
     * Currently, we only support integer type promotions that can be done "implicitly".
     * That is, we know that using a bigger integer tree reader on the original smaller integer
     * column will "just work".
     *
     * In the future, other type promotions might require type conversion.
     */
    // short -> int -> bigint as same integer readers are used for the above types.

    for (int i = 0; i < numReadColumns; i++) {
      OrcProto.Type fColType = fileTypes.get(rowSubtype + i);
      OrcProto.Type rColType = schemaTypes.get(i);
      if (!fColType.getKind().equals(rColType.getKind())) {

        boolean ok = false;
        if (fColType.getKind().equals(OrcProto.Type.Kind.SHORT)) {

          if (rColType.getKind().equals(OrcProto.Type.Kind.INT) ||
              rColType.getKind().equals(OrcProto.Type.Kind.LONG)) {
            // type promotion possible, converting SHORT to INT/LONG requested type
            ok = true;
          }
        } else if (fColType.getKind().equals(OrcProto.Type.Kind.INT)) {

          if (rColType.getKind().equals(OrcProto.Type.Kind.LONG)) {
            // type promotion possible, converting INT to LONG requested type
            ok = true;
          }
        }

        if (!ok) {
          throw new IOException("ORC does not support type conversion from " +
              fColType.getKind().name() + " to " + rColType.getKind().name());
        }
      }
    }

    List<Type> fullSchemaTypes;

    if (isAcid) {
      fullSchemaTypes = new ArrayList<OrcProto.Type>();

      // This copies the ACID struct type which is subtype = 0.
      // It has field names "operation" through "row".
      // And we copy the types for all fields EXCEPT ROW (which must be last!).

      for (int i = 0; i < rowSubtype; i++) {
        fullSchemaTypes.add(fileTypes.get(i).toBuilder().build());
      }

      // Add the row struct type.
      OrcUtils.appendOrcTypesRebuildSubtypes(fullSchemaTypes, schemaTypes, 0);
    } else {
      fullSchemaTypes = schemaTypes;
    }

    int innerStructSubtype = rowSubtype;

    // LOG.info("Schema evolution: (fileTypes) " + fileTypes.toString() +
    //     " (schemaEvolutionTypes) " + schemaEvolutionTypes.toString());

    return new TreeReaderSchema().
        fileTypes(fileTypes).
        schemaTypes(fullSchemaTypes).
        innerStructSubtype(innerStructSubtype);
  }

  private static boolean checkAcidSchema(List<OrcProto.Type> fileSchema) {
    if (fileSchema.get(0).getKind().equals(OrcProto.Type.Kind.STRUCT)) {
      List<String> rootFields = fileSchema.get(0).getFieldNamesList();
      if (acidEventFieldNames.equals(rootFields)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param typeDescr
   * @return ORC types for the ACID event based on the row's type description
   */
  public static List<Type> createEventSchema(TypeDescription typeDescr) {

    List<Type> result = new ArrayList<Type>();

    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    type.setKind(OrcProto.Type.Kind.STRUCT);
    type.addAllFieldNames(acidEventFieldNames);
    for (int i = 0; i < acidEventFieldNames.size(); i++) {
      type.addSubtypes(i + 1);
    }
    result.add(type.build());

    // Automatically add all fields except the last (ROW).
    for (int i = 0; i < acidEventOrcTypeKinds.size() - 1; i ++) {
      type.clear();
      type.setKind(acidEventOrcTypeKinds.get(i));
      result.add(type.build());
    }

    OrcUtils.appendOrcTypesRebuildSubtypes(result, typeDescr);
    return result;
  }

  public static final List<String> acidEventFieldNames= new ArrayList<String>();
  static {
    acidEventFieldNames.add("operation");
    acidEventFieldNames.add("originalTransaction");
    acidEventFieldNames.add("bucket");
    acidEventFieldNames.add("rowId");
    acidEventFieldNames.add("currentTransaction");
    acidEventFieldNames.add("row");
  }
  public static final List<OrcProto.Type.Kind> acidEventOrcTypeKinds =
      new ArrayList<OrcProto.Type.Kind>();
  static {
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.INT);
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.LONG);
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.INT);
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.LONG);
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.LONG);
    acidEventOrcTypeKinds.add(OrcProto.Type.Kind.STRUCT);
  }
}