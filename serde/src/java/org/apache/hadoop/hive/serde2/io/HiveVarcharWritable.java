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

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class HiveVarcharWritable implements WritableComparable<HiveVarcharWritable>{
  protected Text value = new Text();
  transient protected int characterLength = -1;

  public HiveVarcharWritable() {
  }

  public HiveVarcharWritable(HiveVarchar hc) {
    set(hc);
  }

  public HiveVarcharWritable(HiveVarcharWritable hcw) {
    set(hcw);
  }

  public void set(HiveVarchar val) {
    set(val.getValue());
  }

  public void set(String val) {
    set(val, -1);  // copy entire string value
  }

  public void set(HiveVarcharWritable val) {
    value.set(val.value);
    characterLength = val.characterLength;
  }

  public void set(HiveVarcharWritable val, int maxLength) {
    if (val.characterLength > 0 && val.characterLength >= maxLength) {
      set(val);
    } else {
      set(val.getHiveVarchar(), maxLength);
    }
  }

  public void set(HiveVarchar val, int len) {
    set(val.getValue(), len);
  }

  public void set(String val, int maxLength) {
    value.set(HiveBaseChar.enforceMaxLength(val, maxLength));
  }

  public HiveVarchar getHiveVarchar() {
    return new HiveVarchar(value.toString(), -1);
  }

  public int getCharacterLength() {
    if (characterLength < 0) {
      characterLength = getHiveVarchar().getCharacterLength();
    }
    return characterLength;
  }

  public void enforceMaxLength(int maxLength) {
    // Might be possible to truncate the existing Text value, for now just do something simple.
    set(getHiveVarchar(), maxLength);
  }

  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    value.write(out);
  }

  public int compareTo(HiveVarcharWritable rhs) {
    return ShimLoader.getHadoopShims().compareText(value, rhs.value);
  }

  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof HiveVarcharWritable)) {
      return false;
    }
    return value.equals(((HiveVarcharWritable)obj).value);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  public int hashCode() {
    return value.hashCode();
  }

  /**
   * Access to the internal Text member. Use with care.
   * @return
   */
  public Text getTextValue() {
    return value;
  }
}
