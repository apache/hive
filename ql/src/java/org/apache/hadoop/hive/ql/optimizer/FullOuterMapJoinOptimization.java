/*
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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.base.Joiner;

/**
 * FULL OUTER MapJoin planning.
 */
public class FullOuterMapJoinOptimization {

  FullOuterMapJoinOptimization() {
  }

  public static void removeFilterMap(MapJoinDesc mapJoinDesc) throws SemanticException {
    int[][] filterMaps = mapJoinDesc.getFilterMap();
    if (filterMaps == null) {
      return;
    }
    final byte posBigTable = (byte) mapJoinDesc.getPosBigTable();
    final int numAliases = mapJoinDesc.getExprs().size();
    List<TableDesc> valueFilteredTblDescs = mapJoinDesc.getValueFilteredTblDescs();
    for (byte pos = 0; pos < numAliases; pos++) {
      if (pos != posBigTable) {
        int[] filterMap = filterMaps[pos];
        TableDesc tableDesc = valueFilteredTblDescs.get(pos);
        Properties properties = tableDesc.getProperties();
        String columnNameProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
        String columnNameDelimiter =
            properties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
                properties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) :
                  String.valueOf(SerDeUtils.COMMA);

        String columnTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<String> columnNameList;
        if (columnNameProperty.length() == 0) {
          columnNameList = new ArrayList<String>();
        } else {
          columnNameList = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
        }
        List<String> truncatedColumnNameList = columnNameList.subList(0, columnNameList.size() - 1);
        String truncatedColumnNameProperty =
            Joiner.on(columnNameDelimiter).join(truncatedColumnNameList);

        List<TypeInfo> columnTypeList;
        if (columnTypeProperty.length() == 0) {
          columnTypeList = new ArrayList<TypeInfo>();
        } else {
          columnTypeList = TypeInfoUtils
              .getTypeInfosFromTypeString(columnTypeProperty);
        }
        if (!columnTypeList.get(columnTypeList.size() - 1).equals(TypeInfoFactory.shortTypeInfo)) {
          throw new SemanticException("Expecting filterTag smallint as last column type");
        }
        List<TypeInfo> truncatedColumnTypeList =
            columnTypeList.subList(0, columnTypeList.size() - 1);
        String truncatedColumnTypeProperty =
            Joiner.on(",").join(truncatedColumnTypeList);

        properties.setProperty(serdeConstants.LIST_COLUMNS, truncatedColumnNameProperty);
        properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, truncatedColumnTypeProperty);
      }
    }
    mapJoinDesc.setFilterMap(null);
  }
}
