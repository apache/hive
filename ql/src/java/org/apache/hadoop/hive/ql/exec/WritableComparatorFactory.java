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
import org.apache.hadoop.io.WritableComparator;
import java.util.List;
import java.util.Map;

public final class WritableComparatorFactory {
    public static WritableComparator get(Object key, boolean nullSafe, NullOrdering nullOrdering) {
        if (key instanceof List) {
            // STRUCT or ARRAY are expressed as java.util.List
            return new HiveStructComparator(nullSafe, nullOrdering);
        } else if (key instanceof Map) {
            // TODO : https://issues.apache.org/jira/browse/HIVE-25042
            throw new RuntimeException("map datatype is not supported for SMB and Merge Join");
        } else if (key instanceof StandardUnion) {
            throw new RuntimeException("union datatype is not supported for SMB and Merge Join");
        } else {
            return new HiveWritableComparator(nullSafe, nullOrdering);
        }
    }
}
