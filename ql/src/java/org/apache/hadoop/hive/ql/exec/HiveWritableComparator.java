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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.util.ArrayList;
import java.util.LinkedHashMap;

class HiveListComparator extends HiveWritableComparator {
    // For List, all elements will have same type, so only one comparator is sufficient.
    HiveWritableComparator comparator = null;

    @Override
    public int compare(Object key1, Object key2) {
        ArrayList a1 = (ArrayList) key1;
        ArrayList a2 = (ArrayList) key2;
        if (a1.size() != a2.size()) {
            return a1.size() > a2.size() ? 1 : -1;
        }
        if (a1.size() == 0) {
            return 0;
        }

        if (comparator == null) {
            // For List, all elements should be of same type.
            comparator = HiveWritableComparator.get(a1.get(0));
        }

        int result = 0;
        for (int i = 0; i < a1.size(); i++) {
            result = comparator.compare(a1.get(i), a2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }
}

class HiveStructComparator extends HiveWritableComparator {
    HiveWritableComparator[] comparator = null;

    @Override
    public int compare(Object key1, Object key2) {
        ArrayList a1 = (ArrayList) key1;
        ArrayList a2 = (ArrayList) key2;
        if (a1.size() != a2.size()) {
            return a1.size() > a2.size() ? 1 : -1;
        }
        if (a1.size() == 0) {
            return 0;
        }
        if (comparator == null) {
            comparator = new HiveWritableComparator[a1.size()];
            // For struct all elements may not be of same type, so create comparator for each entry.
            for (int i = 0; i < a1.size(); i++) {
                comparator[i] = HiveWritableComparator.get(a1.get(i));
            }
        }
        int result = 0;
        for (int i = 0; i < a1.size(); i++) {
            result = comparator[i].compare(a1.get(i), a2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }
}

class HiveMapComparator extends HiveWritableComparator {
    HiveWritableComparator comparatorValue = null;
    HiveWritableComparator comparatorKey = null;

    @Override
    public int compare(Object key1, Object key2) {
        LinkedHashMap map1 = (LinkedHashMap) key1;
        LinkedHashMap map2 = (LinkedHashMap) key2;
        if (map1.entrySet().size() != map2.entrySet().size()) {
            return map1.entrySet().size() > map2.entrySet().size() ? 1 : -1;
        }
        if (map1.entrySet().size() == 0) {
            return 0;
        }

        if (comparatorKey == null) {
            comparatorKey = HiveWritableComparator.get(map1.keySet().iterator().next());
            comparatorValue = HiveWritableComparator.get(map1.values().iterator().next());
        }

        int result = comparatorKey.compare(map1.keySet().iterator().next(),
                map2.keySet().iterator().next());
        if (result != 0) {
            return result;
        }
        return comparatorValue.compare(map1.values().iterator().next(), map2.values().iterator().next());
    }
}

public class HiveWritableComparator extends WritableComparator {
    private WritableComparator comparator = null;
    public static HiveWritableComparator get(TypeInfo typeInfo) {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return new HiveWritableComparator();
            case LIST:
                return new HiveListComparator();
            case MAP:
                return new HiveMapComparator();
            case STRUCT:
                return new HiveStructComparator();
            case UNION:
            default:
                throw new IllegalStateException("Unexpected value: " + typeInfo.getCategory());
        }
    }

    public static HiveWritableComparator get(Object key) {
        if (key instanceof ArrayList) {
            // For array type struct is used as we do not know if all elements of array are of same type.
            return new HiveStructComparator();
        } else if (key instanceof LinkedHashMap) {
            return new HiveMapComparator();
        } else {
            return new HiveWritableComparator();
        }
    }

    public int compare(Object key1, Object key2) {
        if (comparator == null) {
            comparator = WritableComparator.get(((WritableComparable) key1).getClass());
        }
        return comparator.compare((WritableComparable)key1, (WritableComparable)key2);
    }
}
