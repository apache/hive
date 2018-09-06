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

public class VectorHashKeyWrapperSingleLong extends VectorHashKeyWrapperSingleBase {

  private long longValue0;

  protected VectorHashKeyWrapperSingleLong() {
    super();
    longValue0 = 0;
  }

  @Override
  public void setHashKey() {
    hashcode =
        isNull0 ?
            nullHashcode :
            HashCodeUtil.calculateLongHashCode(longValue0);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperSingleLong) {
      VectorHashKeyWrapperSingleLong keyThat = (VectorHashKeyWrapperSingleLong) that;
      return
          isNull0 == keyThat.isNull0 &&
          longValue0 == keyThat.longValue0;
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperSingleLong clone = new VectorHashKeyWrapperSingleLong();
    clone.isNull0 = isNull0;
    clone.longValue0 = longValue0;
    clone.hashcode = hashcode;
    return clone;
  }

  public void assignLong(int keyIndex, int index, long v) {
    if (keyIndex == 0 && index == 0) {
      isNull0 = false;
      longValue0 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  public void assignLong(int index, long v) {
    if (index == 0) {
      longValue0 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  public void assignNullLong(int keyIndex, int index) {
    if (keyIndex == 0 && index == 0) {
      isNull0 = true;
      longValue0 = 0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  /*
   * This method is mainly intended for debug display purposes.
   */
  @Override
  public String stringifyKeys(VectorColumnSetInfo columnSetInfo)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs [");
    if (!isNull0) {
      sb.append(longValue0);
    } else {
      sb.append("null");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("longs [");
    sb.append(longValue0);
    sb.append("], nulls [");
    sb.append(isNull0);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    if (i == 0) {
      return longValue0;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getVariableSize() {
    return 0;
  }
}
