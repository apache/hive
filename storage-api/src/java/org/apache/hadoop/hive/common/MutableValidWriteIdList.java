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
   * @param writeId write id that is bigger than current highWatermark
   */
  void addOpenWriteId(long writeId);

  /**
   * This method assume the input list is sorted and it will mark them as aborted.
   * @param writeIds a list of write id that is currently open
   */
  void addAbortedWriteIds(List<Long> writeIds);

  /**
   * This method assume the input list is sorted and it will mark them as committed.
   * @param writeIds a list of write id that is currently open
   */
  void addCommittedWriteIds(List<Long> writeIds);
}
