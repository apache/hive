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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable;

import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * The abstract class for vectorized non-match Small Table key iteration.
 */
public abstract class VectorMapJoinNonMatchedIterator {

  protected final MatchTracker matchTracker;

  protected int nonMatchedLogicalSlotNum;

  public VectorMapJoinNonMatchedIterator(MatchTracker matchTracker) {
    this.matchTracker = matchTracker;
  }

  public void init() {
    nonMatchedLogicalSlotNum = -1;
  }

  public boolean findNextNonMatched() {
    throw new RuntimeException("Not implemented");
  }

  public boolean readNonMatchedLongKey() throws HiveException {
    throw new RuntimeException("Not implemented");
  }

  public long getNonMatchedLongKey() throws HiveException {
    throw new RuntimeException("Not implemented");
  }

  public boolean readNonMatchedBytesKey() throws HiveException {
    throw new RuntimeException("Not implemented");
  }

  public byte[] getNonMatchedBytes() {
    throw new RuntimeException("Not implemented");
  }

  public int getNonMatchedBytesOffset() {
    throw new RuntimeException("Not implemented");
  }

  public int getNonMatchedBytesLength() {
    throw new RuntimeException("Not implemented");
  }

  public VectorMapJoinHashMapResult getNonMatchedHashMapResult() {
    throw new RuntimeException("Not implemented");
  }
}
