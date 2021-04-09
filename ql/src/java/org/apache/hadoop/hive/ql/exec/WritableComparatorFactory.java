/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public final class WritableComparatorFactory {
    public static WritableComparator get(TypeInfo typeInfo, boolean nullSafe, NullOrdering nullOrdering) {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return new HiveWritableComparator(nullSafe, nullOrdering);
            case LIST:
                return new HiveListComparator(nullSafe, nullOrdering);
            case MAP:
                return new HiveMapComparator(nullSafe, nullOrdering);
            case STRUCT:
                return new HiveStructComparator(nullSafe, nullOrdering);
            case UNION:
                return new HiveUnionComparator(nullSafe, nullOrdering);
            default:
                throw new IllegalStateException("Unexpected value: " + typeInfo.getCategory());
        }
    }

    public static WritableComparator get(Object key, boolean nullSafe, NullOrdering nullOrdering) {
        if (key instanceof ArrayList) {
            // For array type struct is used as we do not know if all elements of array are of same type.
            return new HiveStructComparator(nullSafe, nullOrdering);
        } else if (key instanceof LinkedHashMap) {
            return new HiveMapComparator(nullSafe, nullOrdering);
        } else if (key instanceof StandardUnion) {
            return new HiveUnionComparator(nullSafe, nullOrdering);
        } else {
            return new HiveWritableComparator(nullSafe, nullOrdering);
        }
    }
}
