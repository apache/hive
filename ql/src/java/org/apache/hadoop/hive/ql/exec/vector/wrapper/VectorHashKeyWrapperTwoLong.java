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

public class VectorHashKeyWrapperTwoLong extends VectorHashKeyWrapperTwoBase {

  private long longValue0;
  private long longValue1;

  protected VectorHashKeyWrapperTwoLong() {
    super();
    longValue0 = 0;
    longValue1 = 0;
  }

  @Override
  public void setHashKey() {
    if (isNull0 || isNull1) {
      hashcode =
          (isNull0 && isNull1 ?
            twoNullHashcode :
            (isNull0 ?
                null0Hashcode ^
                HashCodeUtil.calculateLongHashCode(longValue1) :
                HashCodeUtil.calculateLongHashCode(longValue0) ^
                null1Hashcode));
    } else {
      hashcode =
          HashCodeUtil.calculateLongHashCode(longValue0) >>> 16 ^
          HashCodeUtil.calculateLongHashCode(longValue1);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof VectorHashKeyWrapperTwoLong) {
      VectorHashKeyWrapperTwoLong keyThat = (VectorHashKeyWrapperTwoLong) that;
      return
          isNull0 == keyThat.isNull0 &&
          longValue0 == keyThat.longValue0 &&
          isNull1 == keyThat.isNull1 &&
          longValue1 == keyThat.longValue1;
    }
    return false;
  }

  @Override
  protected Object clone() {
    VectorHashKeyWrapperTwoLong clone = new VectorHashKeyWrapperTwoLong();
    clone.isNull0 = isNull0;
    clone.longValue0 = longValue0;
    clone.isNull1 = isNull1;
    clone.longValue1 = longValue1;
    clone.hashcode = hashcode;
    return clone;
  }

  @Override
  public void assignLong(int keyIndex, int index, long v) {
    if (keyIndex == 0 && index == 0) {
      isNull0 = false;
      longValue0 = v;
    } else if (keyIndex == 1 && index == 1) {
      isNull1 = false;
      longValue1 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  // FIXME: isNull is not updated; which might cause problems
  @Deprecated
  @Override
  public void assignLong(int index, long v) {
    if (index == 0) {
      longValue0 = v;
    } else if (index == 1) {
      longValue1 = v;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public void assignNullLong(int keyIndex, int index) {
    if (keyIndex == 0 && index == 0) {
      isNull0 = true;
      longValue0 = 0;   // Assign 0 to make equals simple.
    } else if (keyIndex == 1 && index == 1) {
      isNull1 = true;
      longValue1 = 0;   // Assign 0 to make equals simple.
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
    sb.append(", ");
    if (!isNull1) {
      sb.append(longValue1);
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
    sb.append(", ");
    sb.append(longValue1);
    sb.append("], nulls [");
    sb.append(isNull0);
    sb.append(", ");
    sb.append(isNull1);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public long getLongValue(int i) {
    if (i == 0) {
      return longValue0;
    } else if (i == 1) {
      return longValue1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  @Override
  public int getVariableSize() {
    return 0;
  }
}
