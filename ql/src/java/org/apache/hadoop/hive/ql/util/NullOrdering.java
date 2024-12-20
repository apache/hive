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

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;

/**
 * Enum for converting different Null ordering description types.
 */
public enum NullOrdering {
  NULLS_FIRST(1, HiveParser.TOK_NULLS_FIRST, NullValueOption.MINVALUE, 'a', RelFieldCollation.NullDirection.FIRST),
  NULLS_LAST(0, HiveParser.TOK_NULLS_LAST, NullValueOption.MAXVALUE, 'z', RelFieldCollation.NullDirection.LAST);

  NullOrdering(int code, int token, NullValueOption nullValueOption, char sign, RelFieldCollation.NullDirection direction) {
    this.code = code;
    this.token = token;
    this.nullValueOption = nullValueOption;
    this.sign = sign;
    this.direction = direction;
  }

  private final int code;
  private final int token;
  private final NullValueOption nullValueOption;
  private final char sign;
  private final RelFieldCollation.NullDirection direction;

  public static NullOrdering fromToken(int token) {
    for (NullOrdering nullOrdering : NullOrdering.values()) {
      if (nullOrdering.token == token) {
        return nullOrdering;
      }
    }
    throw new EnumConstantNotPresentException(NullOrdering.class, "No enum constant present with token " + token);
  }

  public static NullOrdering fromCode(int code) {
    for (NullOrdering nullOrdering : NullOrdering.values()) {
      if (nullOrdering.code == code) {
        return nullOrdering;
      }
    }
    throw new EnumConstantNotPresentException(NullOrdering.class, "No enum constant present with code " + code);
  }

  public static NullOrdering fromSign(char sign) {
    for (NullOrdering nullOrdering : NullOrdering.values()) {
      if (nullOrdering.sign == sign) {
        return nullOrdering;
      }
    }
    throw new EnumConstantNotPresentException(NullOrdering.class, "No enum constant present with sign " + sign);
  }

  public static NullOrdering fromDirection(RelFieldCollation.NullDirection nullDirection) {
    for (NullOrdering nullOrdering : NullOrdering.values()) {
      if (nullOrdering.direction == nullDirection) {
        return nullOrdering;
      }
    }
    throw new EnumConstantNotPresentException(
            NullOrdering.class, "No enum constant present with null direction " + nullDirection);
  }

  public static NullOrdering defaultNullOrder(Configuration hiveConf) {
    return HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_DEFAULT_NULLS_LAST) ?
            NullOrdering.NULLS_LAST : NullOrdering.NULLS_FIRST;
  }

  public int getCode() {
    return code;
  }

  public int getToken() {
    return token;
  }

  public NullValueOption getNullValueOption() {
    return nullValueOption;
  }

  public char getSign() {
    return sign;
  }

  public RelFieldCollation.NullDirection getDirection() {
    return direction;
  }
}
