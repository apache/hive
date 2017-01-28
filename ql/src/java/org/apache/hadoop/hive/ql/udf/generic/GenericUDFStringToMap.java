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

import java.util.LinkedHashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * GenericUDFStringToMap.
 *
 */
@Description(name = "str_to_map", value = "_FUNC_(text, delimiter1, delimiter2) - "
    + "Creates a map by parsing text ", extended = "Split text into key-value pairs"
    + " using two delimiters. The first delimiter separates pairs, and the"
    + " second delimiter sperates key and value. If only one parameter is given, default"
    + " delimiters are used: ',' as delimiter1 and ':' as delimiter2.")
public class GenericUDFStringToMap extends GenericUDF {
  // Must be deterministic order map for consistent q-test output across Java versions - see HIVE-9161
  private final LinkedHashMap<Object, Object> ret = new LinkedHashMap<Object, Object>();
  private transient Converter soi_text, soi_de1 = null, soi_de2 = null;
  final static String default_de1 = ",";
  final static String default_de2 = ":";

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    for (int idx = 0; idx < Math.min(arguments.length, 3); ++idx) {
      if (arguments[idx].getCategory() != Category.PRIMITIVE
          || PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
              ((PrimitiveObjectInspector) arguments[idx]).getPrimitiveCategory())
              != PrimitiveGrouping.STRING_GROUP) {
        throw new UDFArgumentException("All argument should be string/character type");
      }
    }
    soi_text = ObjectInspectorConverters.getConverter(arguments[0],
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    if (arguments.length > 1) {
      soi_de1 = ObjectInspectorConverters.getConverter(arguments[1],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    if (arguments.length > 2) {
      soi_de2 = ObjectInspectorConverters.getConverter(arguments[2],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    ret.clear();
    String text = (String) soi_text.convert(arguments[0].get());
    if (text == null) {
      return ret;
    }

    String delimiter1 = (soi_de1 == null) ?
      default_de1 : (String) soi_de1.convert(arguments[1].get());
    String delimiter2 = (soi_de2 == null) ?
      default_de2 : (String) soi_de2.convert(arguments[2].get());

    if (delimiter1 == null) {
      delimiter1 = default_de1;
    }

    if (delimiter2 == null) {
      delimiter2 = default_de2;
    }

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
    assert (children.length <= 3);
    return getStandardDisplayString("str_to_map", children, ",");
  }
}
