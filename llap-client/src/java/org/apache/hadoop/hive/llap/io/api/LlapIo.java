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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
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
   *                org.apache.hadoop.hive.ql.io.HdfsUtils#getFileId()
   * @return The tail of the ORC file
   * @throws IOException ex
   */
  OrcTail getOrcTailFromCache(Path path, Configuration conf, CacheTag tag, @Nullable Object fileKey) throws IOException;


  /**
   * Returns the metadata buffers associated with the Parquet file on the given path.
   * Content is either obtained from cache, or from disk if there is a cache miss.
   * @param path Parquet file path
   * @param conf jobConf
   * @param fileKey fileId of the Parquet file (either the Long fileId of HDFS or the SyntheticFileId).
   *                Optional, if it is not provided, it will be generated, see:
   *                org.apache.hadoop.hive.ql.io.HdfsUtils#getFileId()
   * @return
   * @throws IOException
   */
  MemoryBufferOrBuffers getParquetFooterBuffersFromCache(Path path, JobConf conf, @Nullable Object fileKey)
      throws IOException;

  /**
   * Handles request to evict entities specified in the request object.
   * @param protoRequest lists Hive entities (DB, table, etc..) whose LLAP buffers should be evicted.
   * @return number of evicted bytes.
   */
  long evictEntity(LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest);

  void initCacheOnlyInputFormat(InputFormat<?, ?> inputFormat);

  /**
   * Creates an LLAP record reader for a given file, by creating a split from this file, and passing it into a new
   * LLAP record reader. May return null when attempting with unsupported schema evolution between reader and file
   * schemas.
   * Useful if a file is read multiple times as LLAP will cache it. Should be used for rather smaller files.
   * @param fileKey - file ID, if null it will be determined during read but for additional performance cost
   * @param path - path for the file to read
   * @param tag - cache tag associated with this file (required for LLAP administrative purposes)
   * @param tableIncludedCols - list of column #'s to be read from the file
   * @param conf - job conf for this read. Schema serialized herein should be aligned with tableIncludedCols
   * @param offset - required offset to start reading from
   * @param length - required reading length
   * @return
   * @throws IOException
   */
  RecordReader<NullWritable, VectorizedRowBatch> llapVectorizedOrcReaderForPath(Object fileKey, Path path, CacheTag tag,
      List<Integer> tableIncludedCols, JobConf conf, long offset, long length, Reporter reporter) throws IOException;

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
