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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.hbase.struct.HBaseValueFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

// Does same thing with LazyFactory#createLazyObjectInspector except that this replaces
// original keyOI with OI which is create by HBaseKeyFactory provided by serde property for hbase
public class HBaseLazyObjectFactory {

  public static ObjectInspector createLazyHBaseStructInspector(
      LazySerDeParameters serdeParams, int index, HBaseKeyFactory keyFactory, List<HBaseValueFactory> valueFactories) throws SerDeException {
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      if (i == index) {
        columnObjectInspectors.add(keyFactory.createKeyObjectInspector(columnTypes.get(i)));
      } else {
        columnObjectInspectors.add(valueFactories.get(i).createValueObjectInspector(
            columnTypes.get(i)));
      }
    }
    return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        serdeParams.getColumnNames(), columnObjectInspectors, null, serdeParams.getSeparators()[0],
        serdeParams, ObjectInspectorOptions.JAVA);
  }
}