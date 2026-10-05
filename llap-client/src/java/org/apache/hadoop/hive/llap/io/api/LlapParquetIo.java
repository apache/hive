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

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.mapred.JobConf;

/**
 * The Parquet-specific half of {@link LlapIo}, reached through {@link LlapIo#parquet()}. The
 * counterpart of {@link LlapOrcIo}; a vectorized-reader-for-path method belongs here too, and
 * will land with the Iceberg caller that needs it.
 */
public interface LlapParquetIo {

  /**
   * Returns the metadata buffers associated with the Parquet file on the given path.
   * Content is either obtained from cache, or from disk if there is a cache miss.
   * @param path Parquet file path
   * @param conf jobConf
   * @param fileKey fileId of the Parquet file (either the Long fileId of HDFS or the SyntheticFileId).
   *                Optional, if it is not provided, it will be generated, see:
   *                org.apache.hadoop.hive.ql.io.HdfsUtils#getFileId()
   * @param cacheHit optional out-parameter; when supplied, its {@code value} field is written
   *                 with {@code true} on a cache hit and {@code false} on a miss, so the caller
   *                 (which lives in llap-server and holds {@code QueryFragmentCounters}) can bump
   *                 {@code METADATA_CACHE_HIT} / {@code METADATA_CACHE_MISS} for the LLAP IO summary.
   * @return
   * @throws IOException
   */
  MemoryBufferOrBuffers getFooterBuffersFromCache(Path path, JobConf conf, @Nullable Object fileKey,
      @Nullable BooleanRef cacheHit) throws IOException;
}
