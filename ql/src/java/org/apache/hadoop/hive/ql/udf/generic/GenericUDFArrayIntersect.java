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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GenericUDFArrayIntersect.
 */
@Description(name = "array_intersect", value =
    "_FUNC_(array1, array2) - Returns an array of the elements in the intersection of array1 and array2,"
        + " without duplicates.", extended = "Example:\n"
    + "  > SELECT _FUNC_(array(1, 2, 3,4), array(1,2,3)) FROM src;" + "\n  [1,2,3]")
public class GenericUDFArrayIntersect extends AbstractGenericUDFArrayBase {
  static final int ARRAY2_IDX = 1;
  private static final String FUNC_NAME = "ARRAY_INTERSECT";
  static final String ERROR_NOT_COMPARABLE = "Input arrays are not comparable to use ARRAY_INTERSECT udf";

  public GenericUDFArrayIntersect() {
    super(FUNC_NAME, 2, 2, ObjectInspector.Category.LIST);
  }

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    ObjectInspector defaultOI = super.initialize(arguments);
    checkArgCategory(arguments, ARRAY2_IDX, ObjectInspector.Category.LIST, FUNC_NAME,
        org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME); //Array1 is already getting validated in Parent class
    if (!ObjectInspectorUtils.compareTypes(arrayOI.getListElementObjectInspector(),
        ((ListObjectInspector) arguments[ARRAY2_IDX]).getListElementObjectInspector())) {
      // check if elements of arrays are comparable
      throw new UDFArgumentTypeException(1, ERROR_NOT_COMPARABLE);
    }
    return defaultOI;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[ARRAY_IDX].get();
    Object array2 = arguments[ARRAY2_IDX].get();
    if (array == null || array2 == null) {
      return null;
    }
    //Copy the first input array into a new list
    List<?> resultArray = new ArrayList<>(((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array));
    resultArray.retainAll(((ListObjectInspector) argumentOIs[ARRAY2_IDX]).getList(array2));
    return resultArray.stream().distinct().map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}
