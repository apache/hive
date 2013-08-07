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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * Generic UDF for array sort
 * <code>SORT_ARRAY(array(obj1, obj2, obj3...))</code>.
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "sort_array",
    value = "_FUNC_(array(obj1, obj2,...)) - "
    + "Sorts the input array in ascending order according to the natural ordering"
    + " of the array elements.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(array('b', 'd', 'c', 'a')) FROM src LIMIT 1;\n"
    + "  'a', 'b', 'c', 'd'")
public class GenericUDFSortArray extends GenericUDF {
  private transient Converter[] converters;
  private final List<Object> ret = new ArrayList<Object>();
  private transient ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
        "The function SORT_ARRAY(array(obj1, obj2,...)) needs one argument.");
    }

    switch(arguments[0].getCategory()) {
      case LIST:
        if(((ListObjectInspector)(arguments[0])).getListElementObjectInspector()
          .getCategory().equals(Category.PRIMITIVE)) {
          break;
        }
      default:
        throw new UDFArgumentTypeException(0, "Argument 1"
          + " of function SORT_ARRAY must be " + serdeConstants.LIST_TYPE_NAME
          + "<" + Category.PRIMITIVE + ">, but " + arguments[0].getTypeName()
          + " was found.");
    }

    ObjectInspector elementObjectInspector =
      ((ListObjectInspector)(arguments[0])).getListElementObjectInspector();
    argumentOIs = arguments;
    converters = new Converter[arguments.length];
    ObjectInspector returnOI = returnOIResolver.get();
    if (returnOI == null) {
      returnOI = elementObjectInspector;
    }
    converters[0] = ObjectInspectorConverters.getConverter(elementObjectInspector, returnOI);

    return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    Object array = arguments[0].get();
    ListObjectInspector arrayOI = (ListObjectInspector) argumentOIs[0];
    List retArray = (List) arrayOI.getList(array);
    final ObjectInspector valInspector = arrayOI.getListElementObjectInspector();
    Collections.sort(retArray, new Comparator() {

      @Override
      public int compare(Object o1, Object o2) {
        return ObjectInspectorUtils.compare(o1, valInspector, o2, valInspector);
      }
    });

    ret.clear();
    for (int i = 0; i < retArray.size(); i++) {
      ret.add(converters[0].convert(retArray.get(i)));
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "sort_array(" + children[0] + ")";
 }
}
