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

package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public abstract class HiveBaseCharWritable {
  protected Text value = new Text();

  public HiveBaseCharWritable() {
  }

  public int getCharacterLength() {
    return getTextUtfLength(value);
  }

  /**
   * Access to the internal Text member. Use with care.
   * @return
   */
  public Text getTextValue() {
    return value;
  }

  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    value.write(out);
  }

  public boolean equals(Object obj) {
    if (obj == null || (obj.getClass() != this.getClass())) {
      return false;
    }
    return value.equals(((HiveBaseCharWritable)obj).value);
  }

  public int hashCode() {
    return value.hashCode();
  }

  /**
   * Checks if b is the first byte of a UTF-8 character.
   *
   */
  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }

  public static int getTextUtfLength(Text t) {
    byte[] data = t.getBytes();
    int len = 0;
    for (int i = 0; i < t.getLength(); i++) {
      if (isUtfStartByte(data[i])) {
        len++;
      }
    }
    return len;
  }
}
