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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Generic UDF to find out max from array elements
 * <code>ARRAY_MAX(array(obj1, obj2, obj3...))</code>.
 *
 * @see GenericUDF
 */
@Description(name = "array_max",
        value = "_FUNC_(array(obj1, obj2,...)) - "
                + "The function returns the maximum value in array with elements for which order is supported",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(array(1, 3, 0, NULL)) FROM src LIMIT 1;\n"
                + "  3")
public class GenericUDFArrayMax extends AbstractGenericUDFArrayBase {

    //Initialise parent member variables
    public GenericUDFArrayMax() {
        super("ARRAY_MAX", 1, 1, ObjectInspector.Category.PRIMITIVE);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[ARRAY_IDX].get();

        if (arrayOI.getListLength(array) <= 0) {
            return null;
        }

        List retArray = ((ListObjectInspector) argumentOIs[ARRAY_IDX]).getList(array);
        Optional value = retArray.stream().filter(Objects::nonNull).max(Comparator.naturalOrder());
        return value.isPresent() ? value.get() : null;
    }
}