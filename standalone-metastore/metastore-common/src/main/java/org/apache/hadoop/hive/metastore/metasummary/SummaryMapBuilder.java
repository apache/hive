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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.metasummary;

import java.util.HashMap;
import java.util.Map;

public class SummaryMapBuilder {
  private final Map<String, Object> container = new HashMap<>();

  public SummaryMapBuilder add(String key, Object value) {
    container.put(key, value);
    return this;
  }

  public Map<String, Object> build() {
    Map<String, Object> result = new HashMap<>(container);
    container.clear();
    return result;
  }

  public <T> T get(String key, Class<T> type) {
    if (!container.containsKey(key)) {
      return null;
    }
    return type.cast(container.get(key));
  }
}
