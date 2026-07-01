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

package org.apache.hadoop.hive.ql.anon.anonymize;

import org.apache.hadoop.hive.ql.anon.policy.DataErasureRule;

final class RuleValues {

  private RuleValues() {}

  static boolean isReplace(final DataErasureRule rule) {
    return rule != null && "REPLACE".equals(rule.resolvedAction());
  }

  static Object scalarValueFor(final DataErasureRule rule, final String targetTypeName) {
    final boolean replace = isReplace(rule);
    switch (targetTypeName) {
      case "int":
      case "Integer":
        return replace ? Integer.valueOf(parseInt(rule)) : Integer.valueOf(0);
      case "short":
      case "Short":
        return replace ? Short.valueOf(parseShort(rule)) : Short.valueOf((short) 0);
      case "long":
      case "Long":
        return replace ? Long.valueOf(parseLong(rule)) : Long.valueOf(0L);
      case "String":
        return replace ? rule.value : "";
      default:
        throw new RuntimeException("unsupported scalar type for rule '"
            + rule.path + "': " + targetTypeName);
    }
  }

  static String stringListElementValueFor(final DataErasureRule rule) {
    return isReplace(rule) ? rule.value : "";
  }

  private static int parseInt(final DataErasureRule rule) {
    try {
      return Integer.parseInt(rule.value);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("REPLACE literal '" + rule.value
          + "' for field '" + rule.path + "' is not an int");
    }
  }

  private static short parseShort(final DataErasureRule rule) {
    try {
      return Short.parseShort(rule.value);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("REPLACE literal '" + rule.value
          + "' for field '" + rule.path + "' is not a short");
    }
  }

  private static long parseLong(final DataErasureRule rule) {
    try {
      return Long.parseLong(rule.value);
    } catch (NumberFormatException nfe) {
      throw new RuntimeException("REPLACE literal '" + rule.value
          + "' for field '" + rule.path + "' is not a long");
    }
  }
}
