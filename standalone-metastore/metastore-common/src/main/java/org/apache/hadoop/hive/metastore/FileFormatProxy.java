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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Metastore.SplitInfos;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

/**
 * Same as PartitionExpressionProxy, but for file format specific methods for metadata cache.
 */
public interface FileFormatProxy {

  /**
   * Applies SARG to file metadata, and produces some result for this file.
   * @param sarg SARG
   * @param fileMetadata File metadata from metastore cache.
   * @return The result to return to client for this file, or null if file is eliminated.
   */
  SplitInfos applySargToMetadata(SearchArgument sarg, ByteBuffer fileMetadata,
      Configuration conf) throws IOException;

  /**
   * @param fs The filesystem of the file.
   * @param path The file path.
   * @param addedVals Output parameter; additional column values for columns returned by
   *                  getAddedColumnsToCache to cache in MS.
   * @return The ORC file metadata for a given file.
   */
  ByteBuffer getMetadataToCache(
      FileSystem fs, Path path, ByteBuffer[] addedVals) throws IOException;

  /**
   * @return Additional column names to cache in MS for this format.
   */
  ByteBuffer[] getAddedColumnsToCache();

  /**
   * @param metadata File metadatas.
   * @return Additional values for columns returned by getAddedColumnsToCache to cache in MS
   *         for respective metadatas.
   */
  ByteBuffer[][] getAddedValuesToCache(List<ByteBuffer> metadata);

}