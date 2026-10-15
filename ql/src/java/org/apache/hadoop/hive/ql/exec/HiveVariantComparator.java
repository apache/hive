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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.io.WritableComparator;

/**
 * A WritableComparator to compare VARIANT objects in the order of their BinarySortable keys: the metadata bytes,
 * then the value bytes, each compared unsigned with a proper prefix first.
 */
final class HiveVariantComparator extends HiveWritableComparator {

    HiveVariantComparator(boolean nullSafe, NullOrdering nullOrdering) {
        super(nullSafe, nullOrdering);
    }

    @Override
    public int compare(Object key1, Object key2) {
        int result = checkNull(key1, key2);
        if (result != not_null) {
            return result;
        }

        Variant v1 = (Variant) key1;
        Variant v2 = (Variant) key2;
        result = compareBytes(v1.getMetadata(), v2.getMetadata());
        return result != 0 ? result : compareBytes(v1.getValue(), v2.getValue());
    }

    private static int compareBytes(byte[] b1, byte[] b2) {
        return WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
    }
}
