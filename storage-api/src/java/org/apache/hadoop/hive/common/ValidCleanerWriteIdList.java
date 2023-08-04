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

import java.util.BitSet;
import java.util.Optional;

/**
 * An implementation of {@link ValidWriteIdList} for use by the Cleaner.
 * Uses the same logic as {@link ValidReaderWriteIdList} with he only exception: 
 * returns NONE for any range that includes any unresolved write ids or write id above {@code highWatermark}
 */
public class ValidCleanerWriteIdList extends ValidReaderWriteIdList {

  public ValidCleanerWriteIdList(ValidReaderWriteIdList vrwil) {
    super(vrwil.getTableName(), vrwil.exceptions, vrwil.abortedBits, vrwil.getHighWatermark(),
      Optional.ofNullable(vrwil.getMinOpenWriteId()).orElse(Long.MAX_VALUE));
  }

  public ValidCleanerWriteIdList(String tableName, long highWatermark) {
    super(tableName, new long[0], new BitSet(), highWatermark, Long.MAX_VALUE);
  }


  /**
   * Returns NONE if some of the write ids in the range are not resolved, 
   * otherwise uses {@link ValidReaderWriteIdList#isWriteIdRangeValid(long, long)} 
   */
  @Override
  public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
    if (maxWriteId > highWatermark) {
      return RangeResponse.NONE;
    }
    return super.isWriteIdRangeValid(minWriteId, maxWriteId);
  }
}
