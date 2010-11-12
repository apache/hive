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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * GenericUDFStringToMap.
 *
 */
@Description(name = "str_to_map", value = "_FUNC_(text, delimiter1, delimiter2) - "
    + "Creates a map by parsing text ", extended = "Split text into key-value pairs"
    + " using two delimiters. The first delimiter seperates pairs, and the"
    + " second delimiter sperates key and value. If only one parameter is given, default"
    + " delimiters are used: ',' as delimiter1 and '=' as delimiter2.")
public class GenericUDFStringToMap extends GenericUDF {
  HashMap<Object, Object> ret = new HashMap<Object, Object>();
  StringObjectInspector soi_text, soi_de1 = null, soi_de2 = null;
  final static String default_de1 = ",";
  final static String default_de2 = ":";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    if (!TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[0]).equals(
        TypeInfoFactory.stringTypeInfo)
        || (arguments.length > 1 &&
            !TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[1]).equals(
            TypeInfoFactory.stringTypeInfo))
        || (arguments.length > 2 &&
            !TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[2]).equals(
            TypeInfoFactory.stringTypeInfo))) {
      throw new UDFArgumentException("All argument should be string");
    }

    soi_text = (StringObjectInspector) arguments[0];
    if (arguments.length > 1) {
      soi_de1 = (StringObjectInspector) arguments[1];
    }
    if (arguments.length > 2) {
      soi_de2 = (StringObjectInspector) arguments[2];
    }

    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ret.clear();
    String text = soi_text.getPrimitiveJavaObject(arguments[0].get());
    String delimiter1 = (soi_de1 == null) ?
      default_de1 : soi_de1.getPrimitiveJavaObject(arguments[1].get());
    String delimiter2 = (soi_de2 == null) ?
      default_de2 : soi_de2.getPrimitiveJavaObject(arguments[2].get());

    String[] keyValuePairs = text.split(delimiter1);

    for (String keyValuePair : keyValuePairs) {
      String[] keyValue = keyValuePair.split(delimiter2, 2);
      if (keyValue.length < 2) {
        ret.put(keyValuePair, null);
      } else {
        ret.put(keyValue[0], keyValue[1]);
      }
    }

    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("str_to_map(");
    assert (children.length <= 3);
    boolean firstChild = true;
    for (String child : children) {
      if (firstChild) {
        firstChild = false;
      } else {
        sb.append(",");
      }
      sb.append(child);
    }
    sb.append(")");
    return sb.toString();
  }
}
