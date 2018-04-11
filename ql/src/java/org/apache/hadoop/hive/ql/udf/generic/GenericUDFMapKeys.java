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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

/**
 * GenericUDFMapKeys.
 *
 */
@Description(name = "map_keys", value = "_FUNC_(map) - "
  + "Returns an unordered array containing the keys of the input map.")
public class GenericUDFMapKeys extends GenericUDF {
  private transient MapObjectInspector mapOI;
  private final ArrayList<Object> retArray = new ArrayList<Object>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
  throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("The function MAP_KEYS only accepts one argument.");
    } else if (!(arguments[0] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(0, "\""
          + Category.MAP.toString().toLowerCase()
          + "\" is expected at function MAP_KEYS, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    mapOI = (MapObjectInspector) arguments[0];
    ObjectInspector mapKeyOI = mapOI.getMapKeyObjectInspector();
    return ObjectInspectorFactory.getStandardListObjectInspector(mapKeyOI);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    retArray.clear();
    Object mapObj = arguments[0].get();
    Map<?,?> mapVal = mapOI.getMap(mapObj);
    if (mapVal != null) {
      retArray.addAll(mapVal.keySet());
    }
    return retArray;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert children.length == 1;
    return getStandardDisplayString("map_keys", children);
  }
}
