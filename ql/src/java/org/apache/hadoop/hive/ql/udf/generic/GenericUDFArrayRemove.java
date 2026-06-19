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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GenericUDFArrayRemove.
 */
@Description(name = "array_remove", value = "_FUNC_(array, element) - Removes all occurrences of element from array.",
    extended = "Example:\n" + "  > SELECT _FUNC_(array(1, 2, 3,4,2), 2) FROM src;\n"
    + "  [1,3,4]")
public class GenericUDFArrayRemove extends AbstractGenericUDFArrayBase {
  private static final String FUNC_NAME = "ARRAY_REMOVE";
  private static final int VALUE_IDX = 1;

  public GenericUDFArrayRemove() {
    super(FUNC_NAME, 2, 2, ObjectInspector.Category.LIST);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    ObjectInspector defaultOI = super.initialize(arguments);
    ObjectInspector arrayElementOI = arrayOI.getListElementObjectInspector();

    ObjectInspector valueOI = arguments[VALUE_IDX];

    // Check if list element and value are of same type
    if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
      throw new UDFArgumentTypeException(VALUE_IDX,
          String.format("%s type element is expected at function array_remove(array<%s>,%s), but %s is found",
              arrayElementOI.getTypeName(), arrayElementOI.getTypeName(), arrayElementOI.getTypeName(),
              valueOI.getTypeName()));
    }
    return defaultOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[ARRAY_IDX].get();
    Object value = arguments[VALUE_IDX].get();
    if (arrayOI.getListLength(array) == 0) {
      return Collections.emptyList();
    } else if (arrayOI.getListLength(array) < 0 || value == null) {
      return null;
    }

    List<?> resultArray = new ArrayList<>(((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array));
    resultArray.removeIf(value::equals);
    return resultArray.stream().map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}
