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

package org.apache.hadoop.hive.common.jsonexplain;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Iterator;

public class JsonUtils {

  public static JSONObject accumulate(JSONObject jsonObject, String key, Object value) {
    if (jsonObject.get(key) == null) {
      if (value instanceof JSONArray) {
        JSONArray newValue = new JSONArray();
        newValue.add(value);
        jsonObject.put(key, newValue);
      } else {
        jsonObject.put(key, value);
      }
    } else {
      Object previous = jsonObject.get(key);
      if (previous instanceof JSONArray) {
        ((JSONArray)previous).add(value);
      } else {
        JSONArray newValue = new JSONArray();
        newValue.add(previous);
        newValue.add(value);
        jsonObject.put(key, newValue);
      }
    }
    return jsonObject;
  }

  public static String[] getNames(JSONObject jsonObject) {
    String[] result = new String[jsonObject.size()];
    Iterator iterator = jsonObject.keySet().iterator();
    for (int i = 0; iterator.hasNext(); i ++) {
      Object key = iterator.next();
      result[i] = key.toString();
    }
    return result;
  }
}
