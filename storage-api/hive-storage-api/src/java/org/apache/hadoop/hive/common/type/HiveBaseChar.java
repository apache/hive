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

  protected HiveBaseChar() {
  }

  /**
   * Sets the string value to a new value, obeying the max length defined for this object.
   * @param val new value
   */
  public void setValue(String val, int maxLength) {
    value = HiveBaseChar.enforceMaxLength(val, maxLength);
  }

  public void setValue(HiveBaseChar val, int maxLength) {
    setValue(val.value, maxLength);
  }

  public static String enforceMaxLength(String val, int maxLength) {
    if (val == null) {
      return null;
    }
    String value = val;

    if (maxLength > 0) {
      int valLength = val.codePointCount(0, val.length());
      if (valLength > maxLength) {
        // Truncate the excess chars to fit the character length.
        // Also make sure we take supplementary chars into account.
        value = val.substring(0, val.offsetByCodePoints(0, maxLength));
      }
    }
    return value;
  }

  public static String getPaddedValue(String val, int maxLength) {
    if (val == null) {
      return null;
    }
    if (maxLength < 0) {
      return val;
    }

    int valLength = val.codePointCount(0, val.length());
    if (valLength > maxLength) {
      return enforceMaxLength(val, maxLength);
    }

    if (maxLength > valLength) {
      // Make sure we pad the right amount of spaces; valLength is in terms of code points,
      // while StringUtils.rpad() is based on the number of java chars.
      int padLength = val.length() + (maxLength - valLength);
      val = StringUtils.rightPad(val, padLength);
    }
    return val;
  }

  public String getValue() {
    return value;
  }

  public int getCharacterLength() {
    return value.codePointCount(0, value.length());
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public String toString() {
    return getValue();
  }
}
