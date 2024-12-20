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
package org.apache.hadoop.hive.serde2.io;

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.common.util.HiveStringUtils;

/**
 * HiveCharWritable.
 * String values will be padded to full char length.
 * Character length, comparison, hashCode should ignore trailing spaces.
 */
public class HiveCharWritable extends HiveBaseCharWritable 
    implements WritableComparable<HiveCharWritable> {

  public HiveCharWritable() {
  }

  public HiveCharWritable(HiveChar hc) {
    set(hc);
  }

  public HiveCharWritable(HiveCharWritable hcw) {
    set(hcw);
  }

  public HiveCharWritable(byte[] bytes, int maxLength) {
    set(bytes, maxLength);
  }

  public void set(byte[] bytes, int maxLength) {
    value.set(bytes);
    enforceMaxLength(maxLength);
  }

  public void set(HiveChar val) {
    set(val.getValue(), -1);
  }

  public void set(String val) {
    set(val, -1);
  }

  public void set(HiveCharWritable val) {
    value.set(val.value);
    charLength = -1;
  }

  public void set(HiveCharWritable val, int maxLength) {
    set(val.getHiveChar(), maxLength);
  }

  public void set(HiveChar val, int len) {
    set(val.getValue(), len);
  }

  public void set(String val, int maxLength) {
    value.set(HiveBaseChar.getPaddedValue(val, maxLength));
  }

  public HiveChar getHiveChar() {
    // Copy string value as-is
    return new HiveChar(value.toString(), -1);
  }

  public void enforceMaxLength(int maxLength) {
    if (getCharacterLength()!=maxLength)
      set(getHiveChar(), maxLength);
  }

  public Text getStrippedValue() {
    if (value.charAt(value.getLength() - 1) != ' ') {
      return value;
    }
    return new Text(HiveCharWritable.stripTrailingWhitespace(value));
  }

  public Text getPaddedValue() {
    return getTextValue();
  }

  public int getCharacterLength() {
    if (charLength != -1) {
      return charLength;
    }
    charLength = HiveStringUtils.getTextUtfLength(getStrippedValue());
    return charLength;
  }

  public int compareTo(HiveCharWritable rhs) {
    return getStrippedValue().compareTo(rhs.getStrippedValue());
  }

  public boolean equals(Object rhs) {
    if (rhs == this) {
      return true;
    }
    if (rhs == null || rhs.getClass() != getClass()) {
      return false;
    }
    return this.getStrippedValue().equals(((HiveCharWritable) rhs).getStrippedValue());
  }

  public int hashCode() {
    return getStrippedValue().hashCode();
  }

  @Override
  public String toString() {
    return getPaddedValue().toString();
  }

  public static byte[] stripTrailingWhitespace(Text valueToStrip) {
    byte[] input = valueToStrip.getBytes();
    int i = input.length;
    // iterate from end while we see space or 0x character
    while (i-- > 0 && (input[i] == 32 || input[i] == 0)) {
    }
    byte[] output = new byte[i + 1];
    System.arraycopy(input, 0, output, 0, i + 1);
    return output;
  }
}
