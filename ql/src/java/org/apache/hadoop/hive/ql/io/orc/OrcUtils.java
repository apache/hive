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
      includeColumnsImpl(results, selectedColumns, allColumns, inspector);
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

}
