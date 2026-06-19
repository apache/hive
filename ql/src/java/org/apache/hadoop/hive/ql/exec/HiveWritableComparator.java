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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class HiveWritableComparator extends WritableComparator {
    private WritableComparator comparator = null;
    protected final transient boolean nullSafe;
    protected final transient NullOrdering nullOrdering;
    protected final int not_null = 2;

    HiveWritableComparator(boolean nullSafe, NullOrdering nullOrdering) {
        this.nullSafe = nullSafe;
        this.nullOrdering = nullOrdering;
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

        // The IEEE 754 floating point spec specifies that signed -0.0 and 0.0 should be treated as equal.
        // Double.compare() and Float.compare() treats -0.0 and 0.0 as different
        if ((key1 instanceof DoubleWritable && key2 instanceof DoubleWritable &&
            ((DoubleWritable) key1).get() == 0.0d && ((DoubleWritable) key2).get() == 0.0d) ||
            (key1 instanceof FloatWritable && key2 instanceof FloatWritable &&
                ((FloatWritable) key1).get() == 0.0f && ((FloatWritable) key2).get() == 0.0f)) {
            return 0;
        }

        if (comparator == null) {
            comparator = WritableComparator.get(((WritableComparable) key1).getClass());
        }
        return comparator.compare((WritableComparable)key1, (WritableComparable)key2);
    }
}
