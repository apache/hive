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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.util.stream.LongStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * HashTableLoader is an interface used by MapJoinOperator used to load the hashtables
 * needed to process the join.
 */
public interface HashTableLoader {

  enum HashTableLoaderCounters {
    HASHTABLE_LOAD_TIME_MS
  };

  void init(ExecMapperContext context, MapredContext mrContext, Configuration hconf,
      MapJoinOperator joinOp);

  void load(MapJoinTableContainer[] mapJoinTables, MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException;

  /**
   * The key count to size a map join hash table for: the smaller of the optimizer's distinct-key
   * estimate and the APPROXIMATE_INPUT_RECORDS counter. Either can be wrong, and the two errors
   * cost differently: the slot arrays are allocated from this value before the first row is read,
   * so sizing too high is resident memory the monitor only sees afterwards, while sizing too low
   * is a rehash of the slot index as rows arrive. The counter counts rows, so it bounds the key
   * count from above whenever it is accurate; it is never a floor on one.
   *
   * @param estKeyCount the optimizer's distinct-key estimate, non-positive when unavailable
   * @param inputRecords the APPROXIMATE_INPUT_RECORDS counter, non-positive when unavailable
   * @return the smallest positive signal, or -1 when there is none. Never 0:
   *         {@link org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper#calculateTableSize}
   *         honours 0 as a real size, which then fails validateCapacity in the fast tables.
   */
  static long keyCountForSizing(long estKeyCount, long inputRecords) {
    return LongStream.of(estKeyCount, inputRecords).filter(count -> count > 0).min().orElse(-1);
  }
}
