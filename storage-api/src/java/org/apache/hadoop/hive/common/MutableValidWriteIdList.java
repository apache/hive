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

import java.util.List;

/**
 * A mutable version {@link ValidWriteIdList}.
 */
public interface MutableValidWriteIdList extends ValidWriteIdList {
  /**
   * This method will mark write ids between highWatermark+1 and the writeId inclusive as open.
   */
  void addOpenWriteId(long writeId);

  /**
   * This method will mark the writeIds as aborted.
   * Note that we cannot abort a write id that is committed.
   */
  void addAbortedWriteIds(List<Long> writeIds);

  /**
   * This method will mark the writeIds as committed.
   * Note that we cannot commit a write id that is aborted.
   */
  void addCommittedWriteIds(List<Long> writeIds);
}
