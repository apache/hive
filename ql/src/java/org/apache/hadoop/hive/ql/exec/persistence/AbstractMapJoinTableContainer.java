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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMapJoinTableContainer implements MapJoinPersistableTableContainer {
  private final Map<String, String> metaData;

  protected static final String THESHOLD_NAME = "threshold";
  protected static final String LOAD_NAME = "load";

  /** Creates metadata for implementation classes' ctors from threshold and load factor. */
  protected static Map<String, String> createConstructorMetaData(int threshold, float loadFactor) {
    Map<String, String> metaData = new HashMap<String, String>();
    metaData.put(THESHOLD_NAME, String.valueOf(threshold));
    metaData.put(LOAD_NAME, String.valueOf(loadFactor));
    return metaData;
  }

  protected AbstractMapJoinTableContainer(Map<String, String> metaData) {
    this.metaData = metaData;
  }

  @Override
  public Map<String, String> getMetaData() {
    return Collections.unmodifiableMap(metaData);
  }

  protected void putMetaData(String key, String value) {
    metaData.put(key, value);
  }
}
