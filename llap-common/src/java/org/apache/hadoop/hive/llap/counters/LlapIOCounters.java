/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.counters;

/**
 * LLAP IO related counters.
 */
public enum LlapIOCounters {
  NUM_VECTOR_BATCHES,
  NUM_DECODED_BATCHES,
  SELECTED_ROWGROUPS,
  NUM_ERRORS,
  ROWS_EMITTED,
  METADATA_CACHE_HIT,
  METADATA_CACHE_MISS,
  CACHE_HIT_BYTES,
  CACHE_MISS_BYTES,
  ALLOCATED_BYTES,
  ALLOCATED_USED_BYTES,
  TOTAL_IO_TIME_NS,
  DECODE_TIME_NS,
  HDFS_TIME_NS,
  CONSUMER_TIME_NS
}
