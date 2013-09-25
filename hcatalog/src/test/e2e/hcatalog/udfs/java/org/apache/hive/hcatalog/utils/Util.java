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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Util {

  static Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();

  static {
    typeMap.put("tinyint", Byte.class);
    typeMap.put("smallint", Short.class);
    typeMap.put("int", Integer.class);
    typeMap.put("bigint", Long.class);
    typeMap.put("float", Float.class);
    typeMap.put("double", Double.class);
    typeMap.put("string", String.class);
    typeMap.put("boolean", Boolean.class);
    typeMap.put("struct<num:int,str:string,dbl:double>", List.class);
    typeMap.put("map<string,string>", Map.class);
    typeMap.put("array<map<string,string>>", List.class);
  }

  public static void die(String expectedType, Object o) throws IOException {
    throw new IOException("Expected " + expectedType + ", got " +
      o.getClass().getName());
  }


  public static String check(String type, Object o) throws IOException {
    if (o == null) {
      return "null";
    }
    if (check(typeMap.get(type), o)) {
      if (type.equals("map<string,string>")) {
        Map<String, String> m = (Map<String, String>) o;
        check(m);
      } else if (type.equals("array<map<string,string>>")) {
        List<Map<String, String>> listOfMaps = (List<Map<String, String>>) o;
        for (Map<String, String> m : listOfMaps) {
          check(m);
        }
      } else if (type.equals("struct<num:int,str:string,dbl:double>")) {
        List<Object> l = (List<Object>) o;
        if (!check(Integer.class, l.get(0)) ||
          !check(String.class, l.get(1)) ||
          !check(Double.class, l.get(2))) {
          die("struct<num:int,str:string,dbl:double>", l);
        }
      }
    } else {
      die(typeMap.get(type).getName(), o);
    }
    return o.toString();
  }

  /**
   * @param m
   * @throws IOException
   */
  public static void check(Map<String, String> m) throws IOException {
    if (m == null) {
      return;
    }
    for (Entry<String, String> e : m.entrySet()) {
      // just access key and value to ensure they are correct
      if (!check(String.class, e.getKey())) {
        die("String", e.getKey());
      }
      if (!check(String.class, e.getValue())) {
        die("String", e.getValue());
      }
    }

  }

  public static boolean check(Class<?> expected, Object actual) {
    if (actual == null) {
      return true;
    }
    return expected.isAssignableFrom(actual.getClass());
  }

}
