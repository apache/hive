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

package org.apache.orc.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.orc.TypeDescription;

/**
 * Take the file types and the (optional) configuration column names/types and see if there
 * has been schema evolution.
 */
public class SchemaEvolution {
  // indexed by reader column id
  private final TypeDescription[] readerFileTypes;
  // indexed by reader column id
  private final boolean[] included;
  private final TypeDescription fileSchema;
  private final TypeDescription readerSchema;
  private boolean hasConversion;
  // indexed by reader column id
  private final boolean[] ppdSafeConversion;

  public SchemaEvolution(TypeDescription fileSchema, boolean[] includedCols) {
    this(fileSchema, null, includedCols);
  }

  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         boolean[] includeCols) {
    this.included = includeCols == null ? null : Arrays.copyOf(includeCols, includeCols.length);
    this.hasConversion = false;
    this.fileSchema = fileSchema;
    if (readerSchema != null) {
      if (checkAcidSchema(fileSchema)) {
        this.readerSchema = createEventSchema(readerSchema);
      } else {
        this.readerSchema = readerSchema;
      }
      this.readerFileTypes = new TypeDescription[this.readerSchema.getMaximumId() + 1];
      buildConversionFileTypesArray(fileSchema, this.readerSchema);
    } else {
      this.readerSchema = fileSchema;
      this.readerFileTypes = new TypeDescription[this.readerSchema.getMaximumId() + 1];
      buildSameSchemaFileTypesArray();
    }
    this.ppdSafeConversion = populatePpdSafeConversion();
  }

  public TypeDescription getReaderSchema() {
    return readerSchema;
  }

  /**
   * Is there Schema Evolution data type conversion?
   * @return
   */
  public boolean hasConversion() {
    return hasConversion;
  }

  public TypeDescription getFileType(TypeDescription readerType) {
    return getFileType(readerType.getId());
  }

  /**
   * Get the file type by reader type id.
   * @param id reader column id
   * @return
   */
  public TypeDescription getFileType(int id) {
    return readerFileTypes[id];
  }

  /**
   * Check if column is safe for ppd evaluation
   * @param colId reader column id
   * @return true if the specified column is safe for ppd evaluation else false
   */
  public boolean isPPDSafeConversion(final int colId) {
    if (hasConversion()) {
      if (colId < 0 || colId >= ppdSafeConversion.length) {
        return false;
      }
      return ppdSafeConversion[colId];
    }

    // when there is no schema evolution PPD is safe
    return true;
  }

  private boolean[] populatePpdSafeConversion() {
    if (fileSchema == null || readerSchema == null || readerFileTypes == null) {
      return null;
    }

    boolean[] result = new boolean[readerSchema.getMaximumId() + 1];
    boolean safePpd = validatePPDConversion(fileSchema, readerSchema);
    result[readerSchema.getId()] = safePpd;
    List<TypeDescription> children = readerSchema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        TypeDescription fileType = getFileType(child.getId());
        safePpd = validatePPDConversion(fileType, child);
        result[child.getId()] = safePpd;
      }
    }
    return result;
  }

  private boolean validatePPDConversion(final TypeDescription fileType,
      final TypeDescription readerType) {
    if (fileType == null) {
      return false;
    }
    if (fileType.getCategory().isPrimitive()) {
      if (fileType.getCategory().equals(readerType.getCategory())) {
        // for decimals alone do equality check to not mess up with precision change
        if (fileType.getCategory().equals(TypeDescription.Category.DECIMAL) &&
            !fileType.equals(readerType)) {
          return false;
        }
        return true;
      }

      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as doubles in ORC's
      // internal index, but when doing predicate evaluation for queries like "select * from
      // orc_float where f = 74.72" the constant on the filter is converted from string -> double
      // so the precisions will be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats or
      // doubles to range predicates.

      // Similarly string -> char and varchar -> char and vice versa is not possible, as ORC stores
      // char with padded spaces in its internal index.
      switch (fileType.getCategory()) {
        case BYTE:
          if (readerType.getCategory().equals(TypeDescription.Category.SHORT) ||
              readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case SHORT:
          if (readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case INT:
          if (readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case STRING:
          if (readerType.getCategory().equals(TypeDescription.Category.VARCHAR)) {
            return true;
          }
          break;
        case VARCHAR:
          if (readerType.getCategory().equals(TypeDescription.Category.STRING)) {
            return true;
          }
          break;
        default:
          break;
      }
    }
    return false;
  }

  void buildConversionFileTypesArray(TypeDescription fileType,
                                     TypeDescription readerType) {
    // if the column isn't included, don't map it
    if (included != null && !included[readerType.getId()]) {
      return;
    }
    boolean isOk = true;
    // check the easy case first
    if (fileType.getCategory() == readerType.getCategory()) {
      switch (readerType.getCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case TIMESTAMP:
        case BINARY:
        case DATE:
          // these are always a match
          break;
        case CHAR:
        case VARCHAR:
          // We do conversion when same CHAR/VARCHAR type but different maxLength.
          if (fileType.getMaxLength() != readerType.getMaxLength()) {
            hasConversion = true;
          }
          break;
        case DECIMAL:
          // We do conversion when same DECIMAL type but different precision/scale.
          if (fileType.getPrecision() != readerType.getPrecision() ||
              fileType.getScale() != readerType.getScale()) {
            hasConversion = true;
          }
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for(int i=0; i < fileChildren.size(); ++i) {
              buildConversionFileTypesArray(fileChildren.get(i), readerChildren.get(i));
            }
          } else {
            isOk = false;
          }
          break;
        }
        case STRUCT: {
          // allow either side to have fewer fields than the other
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() != readerChildren.size()) {
            hasConversion = true;
          }
          int jointSize = Math.min(fileChildren.size(), readerChildren.size());
          for(int i=0; i < jointSize; ++i) {
            buildConversionFileTypesArray(fileChildren.get(i), readerChildren.get(i));
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Unknown type " + readerType);
      }
    } else {
      /*
       * Check for the few cases where will not convert....
       */

      isOk = ConvertTreeReaderFactory.canConvert(fileType, readerType);
      hasConversion = true;
    }
    if (isOk) {
      int id = readerType.getId();
      if (readerFileTypes[id] != null) {
        throw new RuntimeException("reader to file type entry already assigned");
      }
      readerFileTypes[id] = fileType;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "ORC does not support type conversion from file type %s (%d) to reader type %s (%d)",
              fileType.toString(), fileType.getId(),
              readerType.toString(), readerType.getId()));
    }
  }

  /**
   * Use to make a reader to file type array when the schema is the same.
   * @return
   */
  private void buildSameSchemaFileTypesArray() {
    buildSameSchemaFileTypesArrayRecurse(readerSchema);
  }

  void buildSameSchemaFileTypesArrayRecurse(TypeDescription readerType) {
    if (included != null && !included[readerType.getId()]) {
      return;
    }
    int id = readerType.getId();
    if (readerFileTypes[id] != null) {
      throw new RuntimeException("reader to file type entry already assigned");
    }
    readerFileTypes[id] = readerType;
    List<TypeDescription> children = readerType.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        buildSameSchemaFileTypesArrayRecurse(child);
      }
    }
  }

  private static boolean checkAcidSchema(TypeDescription type) {
    if (type.getCategory().equals(TypeDescription.Category.STRUCT)) {
      List<String> rootFields = type.getFieldNames();
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
  public static TypeDescription createEventSchema(TypeDescription typeDescr) {
    TypeDescription result = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createInt())
        .addField("originalTransaction", TypeDescription.createLong())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createLong())
        .addField("currentTransaction", TypeDescription.createLong())
        .addField("row", typeDescr.clone());
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
}
