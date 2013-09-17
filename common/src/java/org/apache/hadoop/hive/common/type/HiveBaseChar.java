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
package org.apache.hadoop.hive.common.type;

import org.apache.commons.lang.StringUtils;

public abstract class HiveBaseChar {
  protected String value;
  protected int characterLength = -1;

  protected HiveBaseChar() {
  }

  /**
   * Sets the string value to a new value, obeying the max length defined for this object.
   * @param val new value
   */
  public void setValue(String val, int maxLength) {
    characterLength = -1;
    value = HiveBaseChar.enforceMaxLength(val, maxLength);
  }

  public void setValue(HiveBaseChar val, int maxLength) {
    if ((maxLength < 0)
        || (val.characterLength > 0 && val.characterLength <= maxLength)) {
      // No length enforcement required, or source length is less than max length.
      // We can copy the source value as-is.
      value = val.value;
      this.characterLength = val.characterLength;
    } else {
      setValue(val.value, maxLength);
    }
  }

  public static String enforceMaxLength(String val, int maxLength) {
    String value = val;

    if (maxLength > 0) {
      int valLength = val.codePointCount(0, val.length());
      if (valLength > maxLength) {
        // Truncate the excess trailing spaces to fit the character length.
        // Also make sure we take supplementary chars into account.
        value = val.substring(0, val.offsetByCodePoints(0, maxLength));
      }
    }
    return value;
  }

  public String getValue() {
    return value;
  }

  public int getCharacterLength() {
    if (characterLength < 0) {
      characterLength = value.codePointCount(0, value.length());
    }
    return characterLength;
  }
}
