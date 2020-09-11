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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * GenericUDTFExplode.
 *
 */
@Description(name = "explode",
    value = "_FUNC_(a) - separates the elements of array a into multiple rows,"
      + " or the elements of a map into multiple rows and columns ")
public class GenericUDTFExplode extends GenericUDTF {

  private transient ObjectInspector inputOI = null;
  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentException("explode() takes only one argument");
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

    switch (args[0].getCategory()) {
    case LIST:
      inputOI = args[0];
      fieldNames.add("col");
      fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
      break;
    case MAP:
      inputOI = args[0];
      fieldNames.add("key");
      fieldNames.add("value");
      fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
      fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
      break;
    default:
      throw new UDFArgumentException("explode() takes an array or a map as a parameter");
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  private transient final Object[] forwardListObj = new Object[1];
  private transient final Object[] forwardMapObj = new Object[2];

  @Override
  public void process(Object[] o) throws HiveException {
    switch (inputOI.getCategory()) {
    case LIST:
      ListObjectInspector listOI = (ListObjectInspector)inputOI;
      List<?> list = listOI.getList(o[0]);
      if (list == null) {
        return;
      }
      for (Object r : list) {
        forwardListObj[0] = r;
        forward(forwardListObj);
      }
      break;
    case MAP:
      MapObjectInspector mapOI = (MapObjectInspector)inputOI;
      Map<?,?> map = mapOI.getMap(o[0]);
      if (map == null) {
        return;
      }
      for (Entry<?,?> r : map.entrySet()) {
        forwardMapObj[0] = r.getKey();
        forwardMapObj[1] = r.getValue();
        forward(forwardMapObj);
      }
      break;
    }
  }

  @Override
  public String toString() {
    return "explode";
  }
}
