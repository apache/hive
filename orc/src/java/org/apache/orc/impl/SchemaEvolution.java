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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.orc.TypeDescription;

/**
 * Take the file types and the (optional) configuration column names/types and see if there
 * has been schema evolution.
 */
public class SchemaEvolution {
  private final Map<TypeDescription, TypeDescription> readerToFile;
  private final boolean[] included;
  private final TypeDescription readerSchema;
  private static final Log LOG = LogFactory.getLog(SchemaEvolution.class);

  public SchemaEvolution(TypeDescription readerSchema, boolean[] included) {
    this.included = included;
    readerToFile = null;
    this.readerSchema = readerSchema;
  }

  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         boolean[] included) throws IOException {
    readerToFile = new HashMap<>(readerSchema.getMaximumId() + 1);
    this.included = included;
    if (checkAcidSchema(fileSchema)) {
      this.readerSchema = createEventSchema(readerSchema);
    } else {
      this.readerSchema = readerSchema;
    }
    buildMapping(fileSchema, this.readerSchema);
  }

  public TypeDescription getReaderSchema() {
    return readerSchema;
  }

  public TypeDescription getFileType(TypeDescription readerType) {
    TypeDescription result;
    if (readerToFile == null) {
      if (included == null || included[readerType.getId()]) {
        result = readerType;
      } else {
        result = null;
      }
    } else {
      result = readerToFile.get(readerType);
    }
    return result;
  }

  void buildMapping(TypeDescription fileType,
                    TypeDescription readerType) throws IOException {
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
          // HIVE-13648: Look at ORC data type conversion edge cases (CHAR, VARCHAR, DECIMAL)
          isOk = fileType.getMaxLength() == readerType.getMaxLength();
          break;
        case DECIMAL:
          // HIVE-13648: Look at ORC data type conversion edge cases (CHAR, VARCHAR, DECIMAL)
          // TODO we don't enforce scale and precision checks, but probably should
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for(int i=0; i < fileChildren.size(); ++i) {
              buildMapping(fileChildren.get(i), readerChildren.get(i));
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
          int jointSize = Math.min(fileChildren.size(), readerChildren.size());
          for(int i=0; i < jointSize; ++i) {
            buildMapping(fileChildren.get(i), readerChildren.get(i));
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
    }
    if (isOk) {
      readerToFile.put(readerType, fileType);
    } else {
      throw new IOException(
          String.format(
              "ORC does not support type conversion from file type %s (%d) to reader type %s (%d)",
              fileType.toString(), fileType.getId(),
              readerType.toString(), readerType.getId()));
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
