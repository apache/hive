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
import java.util.LinkedHashMap;

public class HiveMapComparator extends HiveWritableComparator {
    WritableComparator comparatorValue = null;
    WritableComparator comparatorKey = null;

    HiveMapComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        LinkedHashMap map1 = (LinkedHashMap) key1;
        LinkedHashMap map2 = (LinkedHashMap) key2;
        if (map1.entrySet().size() != map2.entrySet().size()) {
            return map1.entrySet().size() > map2.entrySet().size() ? 1 : -1;
        }
        if (map1.entrySet().size() == 0) {
            return 0;
        }

        if (comparatorKey == null) {
            comparatorKey =
                    WritableComparatorFactory.get(map1.keySet().iterator().next(), nullSafe, nullOrdering);
            comparatorValue =
                    WritableComparatorFactory.get(map1.values().iterator().next(), nullSafe, nullOrdering);
        }

        result = comparatorKey.compare(map1.keySet().iterator().next(),
                map2.keySet().iterator().next());
        if (result != 0) {
            return result;
        }
        return comparatorValue.compare(map1.values().iterator().next(), map2.values().iterator().next());
    }
}

