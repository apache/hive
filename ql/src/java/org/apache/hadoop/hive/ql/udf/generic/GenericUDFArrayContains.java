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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFArrayContains.
 *
 */
@Description(name = "array_contains",
    value="_FUNC_(array, value) - Returns TRUE if the array contains value.",
    extended="Example:\n"
      + "  > SELECT _FUNC_(array(1, 2, 3), 2) FROM src LIMIT 1;\n"
      + "  true")
@NDV(maxNdv = 2)
public class GenericUDFArrayContains extends GenericUDF {

  private static final int ARRAY_IDX = 0;
  private static final int VALUE_IDX = 1;
  private static final int ARG_COUNT = 2; // Number of arguments to this UDF
  private static final String FUNC_NAME = "ARRAY_CONTAINS"; // External Name

  private transient ObjectInspector valueOI;
  private transient ListObjectInspector arrayOI;
  private transient ObjectInspector arrayElementOI;
  private BooleanWritable result;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    // Check if two arguments were passed
    if (arguments.length != ARG_COUNT) {
      throw new UDFArgumentException(
          "The function " + FUNC_NAME + " accepts "
              + ARG_COUNT + " arguments.");
    }

    // Check if ARRAY_IDX argument is of category LIST
    if (!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
      throw new UDFArgumentTypeException(ARRAY_IDX,
          "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
          + "expected at function ARRAY_CONTAINS, but "
          + "\"" + arguments[ARRAY_IDX].getTypeName() + "\" "
          + "is found");
    }

    arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
    arrayElementOI = arrayOI.getListElementObjectInspector();

    valueOI = arguments[VALUE_IDX];

    // Check if list element and value are of same type
    if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
      throw new UDFArgumentTypeException(VALUE_IDX,
          "\"" + arrayElementOI.getTypeName() + "\""
          + " expected at function ARRAY_CONTAINS, but "
          + "\"" + valueOI.getTypeName() + "\""
          + " is found");
    }

    // Check if the comparison is supported for this type
    if (!ObjectInspectorUtils.compareSupported(valueOI)) {
      throw new UDFArgumentException("The function " + FUNC_NAME
          + " does not support comparison for "
          + "\"" + valueOI.getTypeName() + "\""
          + " types");
    }

    result = new BooleanWritable(false);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }



  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    result.set(false);

    Object array = arguments[ARRAY_IDX].get();
    Object value = arguments[VALUE_IDX].get();

    int arrayLength = arrayOI.getListLength(array);

    // Check if array is null or empty or value is null
    if (value == null || arrayLength <= 0) {
      return result;
    }

    // Compare the value to each element of array until a match is found
    for (int i=0; i<arrayLength; ++i) {
      Object listElement = arrayOI.getListElement(array, i);
      if (listElement != null) {
        if (ObjectInspectorUtils.compare(value, valueOI,
            listElement, arrayElementOI) == 0) {
          result.set(true);
          break;
        }
      }
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == ARG_COUNT);
    return "array_contains(" + children[ARRAY_IDX] + ", "
              + children[VALUE_IDX] + ")";
  }
}
