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

package org.apache.hadoop.hive.ql.util;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import com.google.common.collect.ImmutableMap;

/**
 * Utility class for converting different direction description types.
 */
public final class DirectionUtils {
  private DirectionUtils() {
    throw new UnsupportedOperationException("DirectionUtils should not be instantiated");
  }

  public static final int ASCENDING_CODE = 1;
  public static final int DESCENDING_CODE = 0;

  private static final ImmutableMap<String, Object> ASCENDING_DATA = ImmutableMap.of(
      "code", ASCENDING_CODE,
      "sign", '+',
      "text", "ASC",
      "direction", Direction.ASCENDING,
      "token", HiveParser.TOK_TABSORTCOLNAMEASC
  );

  private static final ImmutableMap<String, Object> DESCENDING_DATA = ImmutableMap.of(
      "code", DESCENDING_CODE,
      "sign", '-',
      "text", "DESC",
      "direction", Direction.DESCENDING,
      "token", HiveParser.TOK_TABSORTCOLNAMEDESC
  );

  public static String codeToText(int code) {
    return (String)convert("code", code, "text");
  }

  public static char codeToSign(int code) {
    return (char)convert("code", code, "sign");
  }

  public static int tokenToCode(int token) {
    return (int)convert("token", token, "code");
  }

  public static int signToCode(char sign) {
    return (int)convert("sign", sign, "code");
  }

  public static Direction codeToDirection(int code) {
    return (Direction)convert("code", code, "direction");
  }

  public static int directionToCode(Direction direction) {
    return (int)convert("direction", direction, "code");
  }

  private static Object convert(String typeFrom, Object value, String typeTo) {
    Object ascObject = ASCENDING_DATA.get(typeFrom);
    Object descObject = DESCENDING_DATA.get(typeFrom);
    if (ascObject.equals(value)) {
      return ASCENDING_DATA.get(typeTo);
    } else if (descObject.equals(value)) {
      return DESCENDING_DATA.get(typeTo);
    }

    throw new IllegalArgumentException("The value " + value + " isn not a valid value for " + typeFrom);
  }
}
