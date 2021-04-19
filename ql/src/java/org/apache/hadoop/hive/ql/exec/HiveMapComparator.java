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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Iterator;
import java.util.Map;

final class HiveMapComparator extends HiveWritableComparator {
    private WritableComparator comparatorValue = null;
    private WritableComparator comparatorKey = null;

    HiveMapComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        Map map1 = (Map) key1;
        Map map2 = (Map) key2;
        if (comparatorKey == null) {
            comparatorKey =
                    WritableComparatorFactory.get(map1.keySet().iterator().next(), nullSafe, nullOrdering);
            comparatorValue =
                    WritableComparatorFactory.get(map1.values().iterator().next(), nullSafe, nullOrdering);
        }

        Iterator map1KeyIterator = map1.keySet().iterator();
        Iterator map2KeyIterator = map2.keySet().iterator();
        Iterator map1ValueIterator = map1.values().iterator();
        Iterator map2ValueIterator = map2.values().iterator();

        // For map of size greater than 1, the ordering is based on the key value. If key values are same till the
        // size of smaller map, then the size is checked for ordering.
        int size = map1.size() > map2.size() ? map2.size() : map1.size();
        for (int i = 0; i < size; i++) {
            result = comparatorKey.compare(map1KeyIterator.next(), map2KeyIterator.next());
            if (result != 0) {
                return result;
            }
            result = comparatorValue.compare(map1ValueIterator.next(), map2ValueIterator.next());
            if (result != 0) {
                return result;
            }
        }
        return map1.size() == map2.size() ? 0 : map1.size() > map2.size() ? 1 : -1;
    }
}

