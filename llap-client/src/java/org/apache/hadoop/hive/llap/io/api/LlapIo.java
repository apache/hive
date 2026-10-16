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

package org.apache.hadoop.hive.llap.io.api;

import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;

/**
 * The LLAP IO facade: what a caller can do with the daemon's cache and IO threads regardless of
 * the file format in front of it. Whatever only one format has is on that format's own interface,
 * {@link #orc()} or {@link #parquet()}, so adding a format adds an accessor rather than widening
 * this one.
 */
public interface LlapIo<T> {
  InputFormat<NullWritable, T> getInputFormat(
      InputFormat<?, ?> sourceInputFormat, Deserializer serde);
  void close();
  String getMemoryInfo();

  /**
   * purge is best effort and will just release the buffers that are unlocked (refCount == 0). This is typically
   * called when the system is idle.
   */
  long purge();

  /** The ORC-specific operations; never null. */
  LlapOrcIo orc();

  /** The Parquet-specific operations; never null. */
  LlapParquetIo parquet();

  /**
   * Handles request to evict entities specified in the request object.
   * @param protoRequest lists Hive entities (DB, table, etc..) whose LLAP buffers should be evicted.
   * @return number of evicted bytes.
   */
  long evictEntity(LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest);

  void initCacheOnlyInputFormat(InputFormat<?, ?> inputFormat);

  /**
   * Extract and return the cache content metadata.
   */
  LlapDaemonProtocolProtos.CacheEntryList fetchCachedContentInfo();

  /**
   * Load the actual data into the cache based on the provided metadata.
   */
  void loadDataIntoCache(LlapDaemonProtocolProtos.CacheEntryList metadata);

  boolean usingLowLevelCache();

}
