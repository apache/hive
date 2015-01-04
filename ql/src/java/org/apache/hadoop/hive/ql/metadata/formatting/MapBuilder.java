/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.metadata.formatting;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helper class to build Maps consumed by the JSON formatter.  Only
 * add non-null entries to the Map.
 */
public class MapBuilder {
    private Map<String, Object> map = new LinkedHashMap<String, Object>();

    private MapBuilder() {}

    public static MapBuilder create() {
        return new MapBuilder();
    }

    public MapBuilder put(String name, Object val) {
        if (val != null)
            map.put(name, val);
        return this;
    }

    public MapBuilder put(String name, boolean val) {
        map.put(name, Boolean.valueOf(val));
        return this;
    }

    public MapBuilder put(String name, int val) {
        map.put(name, Integer.valueOf(val));
        return this;
    }

    public MapBuilder put(String name, long val) {
        map.put(name, Long.valueOf(val));
        return this;
    }

    public <T> MapBuilder put(String name, T val, boolean use) {
        if (use)
            put(name, val);
        return this;
    }

    public Map<String, Object> build() {
        return map;
    }
}
