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

package org.apache.hive.service.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Possible values of SASL quality-of-protection value.
 */
public enum SaslQOP {
  AUTH("auth"), // Authentication only.
  AUTH_INT("auth-int"), // Authentication and integrity checking by using signatures.
  AUTH_CONF("auth-conf"); // Authentication, integrity and confidentiality checking
                          // by using signatures and encryption.

  public final String saslQop;

  private static final Map<String, SaslQOP> STR_TO_ENUM = new HashMap<String, SaslQOP>();

  static {
    for (SaslQOP saslQop : values()) {
      STR_TO_ENUM.put(saslQop.toString(), saslQop);
    }
  }

  SaslQOP(String saslQop) {
    this.saslQop = saslQop;
  }

  public String toString() {
    return saslQop;
  }

  public static SaslQOP fromString(String str) {
    if (str != null) {
      str = str.toLowerCase();
    }
    SaslQOP saslQOP = STR_TO_ENUM.get(str);
    if (saslQOP == null) {
      throw new IllegalArgumentException(
        "Unknown auth type: " + str + " Allowed values are: " + STR_TO_ENUM.keySet());
    }
    return saslQOP;
  }
}
