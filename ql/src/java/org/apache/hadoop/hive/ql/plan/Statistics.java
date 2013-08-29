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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * Statistics. Describes the output of an operator in terms of size, rows, etc
 * based on estimates.
 *
 */
@SuppressWarnings("serial")
public class Statistics implements Serializable {

  // only available stat right now is the amount of data flowing out of an
  // operator;
  private long numberOfBytes = -1;

  @Explain(displayName = "Estimated data size in bytes")
  public long getNumberOfBytes() {
    return numberOfBytes;
  }

  public void addNumberOfBytes(long numberOfBytes) {
    if (this.numberOfBytes < 0) {
      this.numberOfBytes = 0;
    }

    this.numberOfBytes += numberOfBytes;
  }

  public void setNumberOfBytes(long l) {
    this.numberOfBytes = l;
  }
}
