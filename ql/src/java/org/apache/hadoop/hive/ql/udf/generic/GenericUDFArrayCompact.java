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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * GenericUDFArrayCompact.
 */
@Description(name = "array_compact", value = "_FUNC_(array) - Removes NULL elements from array.",
    extended = "Example:\n" + "  > SELECT _FUNC_(array(1,NULL,3,NULL,4)) FROM src;\n" + "  [1,3,4]")
public class GenericUDFArrayCompact extends AbstractGenericUDFArrayBase {
  private static final String FUNC_NAME = "ARRAY_COMPACT";

  public GenericUDFArrayCompact() {
    super(FUNC_NAME, 1, 1, ObjectInspector.Category.LIST);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object array = arguments[ARRAY_IDX].get();
    int arrayLength = arrayOI.getListLength(array);
    if (arrayLength == 0) {
      return Collections.emptyList();
    } else if (arrayLength < 0) {
      return null;
    }

    List resultArray = new ArrayList<>(((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array));
    return resultArray.stream().filter(Objects::nonNull).map(o -> converter.convert(o)).collect(Collectors.toList());
  }
}
