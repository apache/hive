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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * GenericUDFArrayPosition.
 */
@Description(name = "array_position", value = "_FUNC_(array, element) - Returns the position of the first occurrence of "
    + "element in array. Array indexing starts at 1. If the element value is NULL, a NULL is returned.", extended =
    "Example:\n" + "  > SELECT _FUNC_(array(1, 2, 3,4,2), 2) FROM src;\n" + "  2")
public class GenericUDFArrayPosition extends AbstractGenericUDFArrayBase {
  static final String FUNC_NAME = "ARRAY_POSITION";
  private static final int ELEMENT_IDX = 1;

  public GenericUDFArrayPosition() {
    super(FUNC_NAME, 2, 2, ObjectInspector.Category.PRIMITIVE);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    super.initialize(arguments);
    checkValueAndListElementTypes(arrayOI.getListElementObjectInspector(),arguments[ELEMENT_IDX],ELEMENT_IDX);
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object array = arguments[ARRAY_IDX].get();
    Object value = arguments[ELEMENT_IDX].get();
    if (arrayOI.getListLength(array) < 0 || value == null) {
      return null;
    }
    List<?> resultArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
    // Handling Varchar type this way as Object comparison between string and varchar will not work
    if ((argumentOIs[ELEMENT_IDX].getTypeName().contains(serdeConstants.VARCHAR_TYPE_NAME)
        || ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getListElementObjectInspector().getTypeName()
        .contains(serdeConstants.VARCHAR_TYPE_NAME))) {
      for (int index = 0; index < resultArray.size(); index++) {
        if (resultArray.get(index).toString().equals(value.toString())) {
          return new IntWritable(index + 1);
        }
      }
    }
    return new IntWritable(resultArray.indexOf(value) + 1);
  }
}
