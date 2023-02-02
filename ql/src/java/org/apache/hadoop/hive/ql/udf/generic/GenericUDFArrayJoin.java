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

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * GenericUDFArrayjoin.
 */
@Description(name = "array_join", value = "_FUNC_(array, delimiter, replaceNull) - concatenate the elements of an array with a specified delimiter", extended =
    "Example:\n" + "  > SELECT _FUNC_(array(1, 2, 3,4), ',') FROM src LIMIT 1;\n" + "  1,2,3,4\n"
        + "  > SELECT _FUNC_(array(1, 2, NULL, 4), ',',':') FROM src LIMIT 1;\n"
        + "  1,2,:,4") public class GenericUDFArrayJoin extends AbstractGenericUDFArrayBase {
  private static final int SEPARATOR_IDX = 1;
  private static final int REPLACE_NULL_IDX = 2;
  private final Text result = new Text();

  public GenericUDFArrayJoin() {
    super("ARRAY_JOIN", 2, 3, ObjectInspector.Category.PRIMITIVE);
  }

  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    super.initialize(arguments);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[ARRAY_IDX].get();

    if (arrayOI.getListLength(array) <= 0) {
      return null;
    }

    List<?> retArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
    String separator = arguments[SEPARATOR_IDX].get().toString();
    if (arguments.length > REPLACE_NULL_IDX && arguments[REPLACE_NULL_IDX].get() != null) {
      result.set(Joiner.on(separator).useForNull(arguments[REPLACE_NULL_IDX].get().toString()).join(retArray));
    } else {
      result.set(Joiner.on(separator).skipNulls().join(retArray));
    }
    return result;
  }
}
