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

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

// Does same thing with LazyFactory#createLazyObjectInspector except that this replaces
// original keyOI with OI which is create by HBaseKeyFactory provided by serde property for hbase
public class HBaseLazyObjectFactory {

  public static ObjectInspector createLazyHBaseStructInspector(
      SerDeParameters serdeParams, int index, HBaseKeyFactory factory) throws SerDeException {
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      if (i == index) {
        columnObjectInspectors.add(factory.createKeyObjectInspector(columnTypes.get(i)));
      } else {
        columnObjectInspectors.add(LazyFactory.createLazyObjectInspector(
            columnTypes.get(i), serdeParams.getSeparators(), 1, serdeParams.getNullSequence(),
            serdeParams.isEscaped(), serdeParams.getEscapeChar()));
      }
    }
    return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        serdeParams.getColumnNames(), columnObjectInspectors, serdeParams.getSeparators()[0],
        serdeParams.getNullSequence(), serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(), serdeParams.getEscapeChar());
  }
}
