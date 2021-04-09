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

class HiveListComparator extends HiveWritableComparator {
    // For List, all elements will have same type, so only one comparator is sufficient.
    HiveWritableComparator comparator = null;

    HiveListComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }
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
            comparator = HiveWritableComparator.get(a1.get(0), nullSafe, nullOrdering);
        }

        result = 0;
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

    HiveStructComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

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
                comparator[i] = HiveWritableComparator.get(a1.get(i), nullSafe, nullOrdering);
            }
        }
        result = 0;
        for (int i = 0; i < a1.size(); i++) {
            result = comparator[i].compare(a1.get(i), a2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return result;
    }
}

class HiveUnionComparator extends HiveWritableComparator {
    HiveWritableComparator comparator = null;

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
            comparator = HiveWritableComparator.get(u1.getObject(), nullSafe, nullOrdering);
        }
        return comparator.compare(u1.getObject(), u2.getObject());
    }
}

class HiveMapComparator extends HiveWritableComparator {
    HiveWritableComparator comparatorValue = null;
    HiveWritableComparator comparatorKey = null;

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
            comparatorKey = HiveWritableComparator.get(map1.keySet().iterator().next(), nullSafe, nullOrdering);
            comparatorValue = HiveWritableComparator.get(map1.values().iterator().next(), nullSafe, nullOrdering);
        }

        result = comparatorKey.compare(map1.keySet().iterator().next(),
                map2.keySet().iterator().next());
        if (result != 0) {
            return result;
        }
        return comparatorValue.compare(map1.values().iterator().next(), map2.values().iterator().next());
    }
}

public class HiveWritableComparator extends WritableComparator {
    private WritableComparator comparator = null;
    protected transient boolean nullSafe;
    transient NullOrdering nullOrdering;
    protected transient int not_null = 2;

    HiveWritableComparator(boolean nullSafe, NullOrdering nullOrdering) {
        this.nullSafe = nullSafe;
        this.nullOrdering = nullOrdering;
    }

    public static HiveWritableComparator get(TypeInfo typeInfo, boolean nullSafe, NullOrdering nullOrdering) {
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

    public static HiveWritableComparator get(Object key, boolean nullSafe, NullOrdering nullOrdering) {
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

    protected int checkNull(Object key1, Object key2) {
        if (key1 == null && key2 == null) {
            if (nullSafe) {
                return 0;
            } else {
                return -1;
            }
        } else if (key1 == null) {
            return nullOrdering == null ? -1 : nullOrdering.getNullValueOption().getCmpReturnValue();
        } else if (key2 == null) {
            return nullOrdering == null ? 1 : -nullOrdering.getNullValueOption().getCmpReturnValue();
        } else {
            return not_null;
        }
    }

    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        if (comparator == null) {
            comparator = WritableComparator.get(((WritableComparable) key1).getClass());
        }
        return comparator.compare((WritableComparable)key1, (WritableComparable)key2);
    }
}
