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

package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class CustomSerDe4 extends CustomSerDe2 {

    @Override
    public void initialize(Configuration conf, Properties tbl)
        throws SerDeException {

      // Read the configuration parameters
      String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
      String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
      final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
          .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
      // The input column can either be a string or a list of integer values.
      List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
      List<TypeInfo> columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
      assert columnNames.size() == columnTypes.size();
      numColumns = columnNames.size();

      // No exception for type checking for simplicity
      // Constructing the row ObjectInspector:
      // The row consists of string columns, double columns, some union<int, double> columns only.
      List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
          columnNames.size());
      for (int c = 0; c < numColumns; c++) {
        if (columnTypes.get(c).equals(TypeInfoFactory.stringTypeInfo)) {
          columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        else if (columnTypes.get(c).equals(TypeInfoFactory.doubleTypeInfo)) {
          columnOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }
        else {

          // Blindly add this as a union type containing int and double!
          // Should be sufficient for the test case.
         List<ObjectInspector> unionOI =  new ArrayList<ObjectInspector>();
         unionOI.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
         unionOI.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
          columnOIs.add(ObjectInspectorFactory.getStandardUnionObjectInspector(unionOI));
        }
      }
      // StandardList uses ArrayList to store the row.
      rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

      // Constructing the row object, etc, which will be reused for all rows.
      row = new ArrayList<String>(numColumns);
      for (int c = 0; c < numColumns; c++) {
        row.add(null);
      }
    }

}
