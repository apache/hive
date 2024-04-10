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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GenericUDFArrayAppend.
 */
@Description(name = "array_append", value = "_FUNC_(array, element) - Returns an array appended by element.",
    extended = "Example:\n" + "  > SELECT _FUNC_(array(1,3,4), 2) FROM src;\n" + "  [1,3,4,2]")
public class GenericUDFArrayAppend extends AbstractGenericUDFArrayBase {
  private static final String FUNC_NAME = "ARRAY_APPEND";
  private static final int ELEMENT_IDX = 1;

  public GenericUDFArrayAppend() {
    super(FUNC_NAME, 2, 2, ObjectInspector.Category.LIST);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    ObjectInspector defaultOI = super.initialize(arguments);
    checkValueAndListElementTypes(arrayElementOI, FUNC_NAME, arguments[ELEMENT_IDX], ELEMENT_IDX);
    return defaultOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object array = arguments[ARRAY_IDX].get();
    Object value = arguments[ELEMENT_IDX].get();
    int arrayLength = arrayOI.getListLength(array);
    if (arrayLength == 0) {
      return Collections.emptyList();
    } else if (arrayLength < 0) {
      return null;
    }

    List resultArray = new ArrayList<>(((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array));
    resultArray.add(value);
    return resultArray.stream().map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}
