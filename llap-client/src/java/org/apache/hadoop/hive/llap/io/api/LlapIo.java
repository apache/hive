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

package org.apache.hadoop.hive.llap.io.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.orc.impl.OrcTail;

import javax.annotation.Nullable;

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

  /**
   * Returns a deserialized OrcTail instance associated with the ORC file on the given path.
   * Raw content is either obtained from cache, or from disk if there is a cache miss.
   * @param path Orc file path
   * @param conf jobConf
   * @param tag a CacheTag instance must be provided as that's needed for cache insertion
   * @param fileKey fileId of the ORC file (either the Long fileId of HDFS or the SyntheticFileId).
   *                Optional, if it is not provided, it will be generated, see:
   *                {@link org.apache.hadoop.hive.ql.io.HdfsUtils.getFileId()}
   * @return The tail of the ORC file
   * @throws IOException ex
   */
  OrcTail getOrcTailFromCache(Path path, Configuration conf, CacheTag tag, @Nullable Object fileKey) throws IOException;

  /**
   * Handles request to evict entities specified in the request object.
   * @param protoRequest lists Hive entities (DB, table, etc..) whose LLAP buffers should be evicted.
   * @return number of evicted bytes.
   */
  long evictEntity(LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest);

  void initCacheOnlyInputFormat(InputFormat<?, ?> inputFormat);
}
