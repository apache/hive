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

package org.apache.hadoop.hive.metastore.utils;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A utility class that can convert a HashMap of Properties into a colon separated string,
 * and can take the same format of string and convert it to a HashMap of Properties.
 */
public class StringableMap extends HashMap<String, String> {

  public StringableMap(String s) {
    String[] parts = s.split(":", 2);
    // read that many chars
    int numElements = Integer.parseInt(parts[0]);
    s = parts[1];
    for (int i = 0; i < numElements; i++) {
      parts = s.split(":", 2);
      int len = Integer.parseInt(parts[0]);
      String key = null;
      if (len > 0) key = parts[1].substring(0, len);
      parts = parts[1].substring(len).split(":", 2);
      len = Integer.parseInt(parts[0]);
      String value = null;
      if (len > 0) value = parts[1].substring(0, len);
      s = parts[1].substring(len);
      put(key, value);
    }
  }

  public StringableMap(Map<String, String> m) {
    super(m);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(size());
    buf.append(':');
    if (size() > 0) {
      for (Map.Entry<String, String> entry : entrySet()) {
        int length = (entry.getKey() == null) ? 0 : entry.getKey().length();
        buf.append(entry.getKey() == null ? 0 : length);
        buf.append(':');
        if (length > 0) buf.append(entry.getKey());
        length = (entry.getValue() == null) ? 0 : entry.getValue().length();
        buf.append(length);
        buf.append(':');
        if (length > 0) buf.append(entry.getValue());
      }
    }
    return buf.toString();
  }

  public Properties toProperties() {
    Properties props = new Properties();
    props.putAll(this);
    return props;
  }
}
