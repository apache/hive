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

package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.serde.serdeConstants;

public class CharTypeInfo  extends BaseCharTypeInfo {
  private static final long serialVersionUID = 1L;

  // no-arg constructor to make kyro happy.
  public CharTypeInfo() {
    super(serdeConstants.CHAR_TYPE_NAME);
  }

  public CharTypeInfo(int length) {
    super(serdeConstants.CHAR_TYPE_NAME, length);
    BaseCharUtils.validateCharParameter(length);
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    CharTypeInfo pti = (CharTypeInfo) other;

    return this.typeName.equals(pti.typeName) && this.getLength() == pti.getLength();
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return getQualifiedName().hashCode();
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

}
