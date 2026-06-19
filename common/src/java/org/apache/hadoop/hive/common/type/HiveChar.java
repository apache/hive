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
package org.apache.hadoop.hive.common.type;

import org.apache.commons.lang3.StringUtils;

/**
 * HiveChar.
 * String values will be padded to full char length.
 * Character length, comparison, hashCode should ignore trailing spaces.
 */
public class HiveChar extends HiveBaseChar
  implements Comparable<HiveChar> {

  public static final int MAX_CHAR_LENGTH = 255;

  public HiveChar() {
  }

  public HiveChar(String val, int len) {
    setValue(val, len);
  }

  public HiveChar(HiveChar hc, int len) {
    setValue(hc.value, len);
  }

  /**
   * Set char value, padding or truncating the value to the size of len parameter.
   */
  @Override
  public void setValue(String val, int len) {
    super.setValue(HiveBaseChar.getPaddedValue(val, len), -1);
  }

  public void setValue(String val) {
    setValue(val, -1);
  }

  public String getStrippedValue() {
    return StringUtils.stripEnd(value, " ");
  }

  public String getPaddedValue() {
    return value;
  }

  @Override
  public int getCharacterLength() {
    String strippedValue = getStrippedValue();
    return strippedValue.codePointCount(0, strippedValue.length());
  }

  @Override
  public String toString() {
    return getPaddedValue();
  }

  @Override
  public int compareTo(HiveChar rhs) {
    if (rhs == this) {
      return 0;
    }
    return this.getStrippedValue().compareTo(rhs.getStrippedValue());
  }

  @Override
  public boolean equals(Object rhs) {
    if (rhs == this) {
      return true;
    }
    if (rhs == null || rhs.getClass() != getClass()) {
      return false;
    }
    return this.getStrippedValue().equals(((HiveChar) rhs).getStrippedValue());
  }

  @Override
  public int hashCode() {
    return getStrippedValue().hashCode();
  }

}
