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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSetInfo;
import org.apache.hive.common.util.HashCodeUtil;

// No need to override assigns - all assign ops will fail due to 0 key size.
public final class VectorHashKeyWrapperEmpty extends VectorHashKeyWrapperBase {

  public final static VectorHashKeyWrapperBase EMPTY_KEY_WRAPPER = new VectorHashKeyWrapperEmpty();

  private static final int emptyHashcode =
      HashCodeUtil.calculateLongHashCode(88L);

  private VectorHashKeyWrapperEmpty() {
    super();
  }

  @Override
  public void setHashKey() {
    hashcode = emptyHashcode;
  }

  @Override
  protected Object clone() {
    // immutable
    return this;
  }

  @Override
  public boolean equals(Object that) {
    if (that == this) {
      // should only be one object
      return true;
    }
    return super.equals(that);
  }

  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    return "";
  }

  @Override
  public String toString()
  {
    return "nulls []";
  }

  @Override
  public int getVariableSize() {
    return 0;
  }

  @Override
  public void clearIsNull() {
    // Nothing to do.
  }

  @Override
  public void setNull() {
    // Nothing to do.
  }
}
