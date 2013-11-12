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

/**
 *
 * HiveVarChar.
 * String wrapper to support SQL VARCHAR features.
 * Max string length is enforced.
 *
 */
public class HiveVarchar extends HiveBaseChar
  implements Comparable<HiveVarchar> {

  public static final int MAX_VARCHAR_LENGTH = 65535;

  public HiveVarchar() {
  }

  public HiveVarchar(String val, int len) {
    setValue(val, len);
  }

  public HiveVarchar(HiveVarchar hc, int len) {
    setValue(hc, len);
  }

  /**
   * Set the new value
   */
  public void setValue(String val) {
    super.setValue(val, -1);
  }

  public void setValue(HiveVarchar hc) {
    super.setValue(hc.getValue(), -1);
  }

  public int compareTo(HiveVarchar rhs) {
    if (rhs == this) {
      return 0;
    }
    return this.getValue().compareTo(rhs.getValue());
  }

  public boolean equals(HiveVarchar rhs) {
    if (rhs == this) {
      return true;
    }
    return this.getValue().equals(rhs.getValue());
  }
}
