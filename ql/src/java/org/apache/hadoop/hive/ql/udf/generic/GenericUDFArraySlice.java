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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GenericUDFArraySlice.
 */
@Description(name = "array_slice", value = "_FUNC_(array, start, length) - Returns the subset or range of elements from"
    + " an array (subarray).", extended = "Example:\n" + "  > SELECT _FUNC_(array(1, 2, 3,4), 2,2) FROM src LIMIT 1;\n"
    + "  3,4")
public class GenericUDFArraySlice extends AbstractGenericUDFArrayBase {
  private static final String FUNC_NAME = "ARRAY_SLICE";
  private static final int START_IDX = 1;
  private static final int LENGTH_IDX = 2;

  public GenericUDFArraySlice() {
    super(FUNC_NAME, 3, 3, ObjectInspector.Category.LIST);
  }

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    ObjectInspector defaultOI = super.initialize(arguments);
    // Check whether start and length inputs are of integer type
    checkArgIntPrimitiveCategory((PrimitiveObjectInspector) arguments[START_IDX], FUNC_NAME, START_IDX);
    checkArgIntPrimitiveCategory((PrimitiveObjectInspector) arguments[LENGTH_IDX], FUNC_NAME, LENGTH_IDX);
    return defaultOI;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[ARRAY_IDX].get();
    if (arrayOI.getListLength(array) == 0) {
      return Collections.emptyList();
    } else if (arrayOI.getListLength(array) < 0) {
      return null;
    }

    List<?> retArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
    int start = ((IntObjectInspector) argumentOIs[START_IDX]).get(arguments[START_IDX].get());
    int length = ((IntObjectInspector) argumentOIs[LENGTH_IDX]).get(arguments[LENGTH_IDX].get());
    // return empty list if start/length are out of range of the array
    if (start + length > retArray.size()) {
      return Collections.emptyList();
    }
    return retArray.subList(start, start + length).stream().map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}
