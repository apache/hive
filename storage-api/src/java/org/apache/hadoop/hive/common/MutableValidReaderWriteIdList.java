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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is a mutable version of {@link ValidReaderWriteIdList} for use by an external cache layer.
 * To use this class, we need to always mark the writeId as open before to mark it as aborted/committed.
 * This class is not thread safe.
 */
public class MutableValidReaderWriteIdList extends ValidReaderWriteIdList implements MutableValidWriteIdList {
  private static final Logger LOG = LoggerFactory.getLogger(MutableValidReaderWriteIdList.class.getName());

  public MutableValidReaderWriteIdList(ValidReaderWriteIdList writeIdList) {
    super(writeIdList.writeToString());
    exceptions = new ArrayList<>(exceptions);
  }

  @Override
  public void addOpenWriteId(long writeId) {
    if (writeId <= highWatermark) {
      LOG.debug("Won't add any open write id because {} is less than or equal to high watermark: {}",
          writeId, highWatermark);
      return;
    }
    for (long currentId = highWatermark + 1; currentId <= writeId; currentId++) {
      exceptions.add(currentId);
    }
    highWatermark = writeId;
  }

  @Override
  public void addAbortedWriteIds(List<Long> writeIds) {
    Preconditions.checkNotNull(writeIds);
    Preconditions.checkArgument(writeIds.size() > 0);
    long maxWriteId = Collections.max(writeIds);
    Preconditions.checkArgument(maxWriteId <= highWatermark,
        "Should mark write id (%s) as open before abort it", maxWriteId);
    for (long writeId: writeIds) {
      int index = Collections.binarySearch(exceptions, writeId);
      // make sure the write id is not committed
      Preconditions.checkState(index >= 0);
      abortedBits.set(index);
    }
    updateMinOpenWriteId();
  }

  @Override
  public void addCommittedWriteIds(List<Long> writeIds) {
    Preconditions.checkNotNull(writeIds);
    Preconditions.checkArgument(writeIds.size() > 0);
    long maxWriteId = Collections.max(writeIds);
    Preconditions.checkArgument(maxWriteId <= highWatermark,
        "Should mark write id (%s) as open before commit it", maxWriteId);
    List<Long> updatedExceptions = new ArrayList<>();
    BitSet updatedAbortedBits = new BitSet();

    Set<Integer> idxToRemove = new HashSet<>();
    for (long writeId: writeIds) {
      int idx = Collections.binarySearch(exceptions, writeId);
      if (idx >= 0) {
        // make sure the write id is open rather than aborted
        Preconditions.checkState(!abortedBits.get(idx));
        idxToRemove.add(idx);
      }
    }
    for (int idx = 0; idx < exceptions.size(); idx++) {
      if (idxToRemove.contains(idx)) {
        continue;
      }
      updatedAbortedBits.set(updatedExceptions.size(), abortedBits.get(idx));
      updatedExceptions.add(exceptions.get(idx));
    }
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
