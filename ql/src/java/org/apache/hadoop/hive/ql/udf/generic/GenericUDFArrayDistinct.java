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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generic UDF for distinct array
 * <code>ARRAY_DISTINCT(array(obj1, obj2, obj3...))</code>.
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "array_distinct", value = "_FUNC_(array(obj1, obj2,...)) - "
    + "The function returns an array of the same type as the input array with distinct values.", extended = "Example:\n"
    + "  > SELECT _FUNC_(array('b', 'd', 'd', 'a')) FROM src LIMIT 1;\n"
    + "  ['b', 'd', 'a']") public class GenericUDFArrayDistinct extends AbstractGenericUDFArrayBase {

  public GenericUDFArrayDistinct() {
    super("ARRAY_DISTINCT", 1, 1, ObjectInspector.Category.LIST);
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[ARRAY_IDX].get();

    // If the array is empty, then there are no duplicates, return back the empty array
    if (arrayOI.getListLength(array) == 0) {
      return Collections.emptyList();
    } else if (arrayOI.getListLength(array) < 0) {
      return null;
    }

    List<?> retArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
    return retArray.stream().distinct().map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}