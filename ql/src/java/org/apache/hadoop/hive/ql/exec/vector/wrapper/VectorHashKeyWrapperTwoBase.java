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

import org.apache.hive.common.util.HashCodeUtil;

public abstract class VectorHashKeyWrapperTwoBase extends VectorHashKeyWrapperBase {

  protected boolean isNull0;
  protected boolean isNull1;

  protected static final int null0Hashcode =
      HashCodeUtil.calculateLongHashCode(8893L);
  protected static final int null1Hashcode =
      HashCodeUtil.calculateLongHashCode(255533L);
  protected static final int twoNullHashcode =
      HashCodeUtil.calculateLongHashCode(7566L);

  protected VectorHashKeyWrapperTwoBase() {
    super();
    isNull0 = false;
    isNull1 = false;
  }

  @Override
  public void clearIsNull() {
    isNull0 = false;
    isNull1 = false;
  }

  @Override
  public void setNull() {
    isNull0 = true;
    isNull1 = true;
  }

  @Override
  public boolean isNull(int keyIndex) {
    if (keyIndex == 0) {
      return isNull0;
    } else if (keyIndex == 1) {
      return isNull1;
    } else {
      throw new ArrayIndexOutOfBoundsException();
    }
  }
}
