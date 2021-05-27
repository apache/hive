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

package org.apache.hadoop.hive.common;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This class is a mutable version of {@link ValidReaderWriteIdList} for use by an external cache layer.
 * To use this class, we need to always mark the writeId as open before to mark it as aborted/committed.
 * This class is not thread safe.
 */
public class MutableValidReaderWriteIdList extends ValidReaderWriteIdList implements MutableValidWriteIdList {
  public MutableValidReaderWriteIdList(ValidReaderWriteIdList writeIdList) {
    super(writeIdList.writeToString());
    exceptions = new ArrayList<>(exceptions);
  }

  @Override
  public void addOpenWriteId(long writeId) {
    Preconditions.checkArgument(writeId > highWatermark);
    for (long currentId = highWatermark + 1; currentId <= writeId; currentId++) {
      exceptions.add(currentId);
    }
    highWatermark = writeId;
  }

  @Override
  public void addAbortedWriteIds(List<Long> writeIds) {
    for (long writeId: writeIds) {
      int index = Collections.binarySearch(exceptions, writeId);
      Preconditions.checkState(index >= 0);
      abortedBits.set(index);
    }
    updateMinOpenWriteId();
  }

  @Override
  public void addCommittedWriteIds(List<Long> writeIds) {
    Preconditions.checkArgument(writeIds.size() > 0);
    List<Long> updatedExceptions = new ArrayList<>();
    BitSet updatedAbortedBits = new BitSet();

    Iterator<Long> itr = writeIds.iterator();
    long nextCommitted = itr.next();
    for (int index = 0; index < exceptions.size(); index++) {
      long writeId = exceptions.get(index);
      if (writeId != nextCommitted) {
        updatedExceptions.add(writeId);
        if (abortedBits.get(index)) {
          updatedAbortedBits.set(updatedExceptions.size() - 1);
        }
      } else {
        Preconditions.checkState(!abortedBits.get(index), "writeId %s is already aborted", writeId);
        nextCommitted = itr.hasNext() ? itr.next() : -1;
      }
    }
    // All the elements in writeIds should already be removed from exceptions, so
    // the iteration has no more elements.
    Preconditions.checkState(!itr.hasNext());
    exceptions = updatedExceptions;
    abortedBits = updatedAbortedBits;
    updateMinOpenWriteId();
  }

  @Override
  public void readFromString(String str) {
    super.readFromString(str);
    exceptions = new ArrayList<>(exceptions);
  }

  @Override
  public ValidReaderWriteIdList updateHighWatermark(long value) {
    highWatermark = Math.max(highWatermark, value);
    return this;
  }

  private void updateMinOpenWriteId() {
    int index = abortedBits.nextClearBit(0);
    if (index >= exceptions.size()) {
      minOpenWriteId = Long.MAX_VALUE;
    } else {
      minOpenWriteId = exceptions.get(index);
    }
  }
}
