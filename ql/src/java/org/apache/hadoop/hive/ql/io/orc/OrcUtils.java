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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
}
