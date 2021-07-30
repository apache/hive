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
import org.apache.hadoop.io.WritableComparator;
import java.util.ArrayList;
import java.util.List;

/**
 * A WritableComparator to compare STRUCT or ARRAY objects.
 */
final class HiveStructComparator extends HiveWritableComparator {
    private final List<WritableComparator> comparators = new ArrayList<>();

    HiveStructComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        List a1 = (List) key1;
        List a2 = (List) key2;
        if (a1.size() != a2.size()) {
            return a1.size() > a2.size() ? 1 : -1;
        }
        if (a1.size() == 0) {
            return 0;
        }
        // For array, the length may not be fixed, so extend comparators on demand
        for (int i = comparators.size(); i < a1.size(); i++) {
            // For struct, all elements may not be of same type, so create comparator for each entry.
            comparators.add(i, WritableComparatorFactory.get(a1.get(i), nullSafe, nullOrdering));
        }
        result = 0;
        for (int i = 0; i < a1.size(); i++) {
            result = comparators.get(i).compare(a1.get(i), a2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }
}
