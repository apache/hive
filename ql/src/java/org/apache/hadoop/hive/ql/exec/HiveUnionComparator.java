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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HiveUnionComparator extends HiveWritableComparator {
    WritableComparator comparator = null;

    HiveUnionComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        StandardUnion u1 = (StandardUnion) key1;
        StandardUnion u2 = (StandardUnion) key2;

        if (u1.getTag() != u2.getTag()) {
            // If tag is not same, the keys may be of different data types. So can not be compared.
            return u1.getTag() > u2.getTag() ? 1 : -1;
        }

        if (comparator == null) {
            comparator = WritableComparatorFactory.get(u1.getObject(), nullSafe, nullOrdering);
        }
        return comparator.compare(u1.getObject(), u2.getObject());
    }
}
