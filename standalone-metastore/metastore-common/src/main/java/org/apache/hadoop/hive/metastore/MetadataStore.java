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

public interface MetadataStore {
  /**
   * @param fileIds file ID list.
   * @param result The ref parameter, used to return the serialized file metadata.
   */
  void getFileMetadata(List<Long> fileIds, ByteBuffer[] result) throws IOException;

  /**
   * @param fileIds file ID list.
   * @param metadataBuffers Serialized file metadata, one per file ID.
   * @param addedCols The column names for additional columns created by file-format-specific
   *                  metadata handler, to be stored in the cache.
   * @param addedVals The values for addedCols; one value per file ID per added column.
   */
  void storeFileMetadata(List<Long> fileIds, List<ByteBuffer> metadataBuffers,
      ByteBuffer[] addedCols, ByteBuffer[][] addedVals) throws IOException, InterruptedException;

  /**
   * @param fileId The file ID.
   * @param metadata Serialized file metadata.
   * @param addedCols The column names for additional columns created by file-format-specific
   *                  metadata handler, to be stored in the cache.
   * @param addedVals The values for addedCols; one value per added column.
   */
  void storeFileMetadata(long fileId, ByteBuffer metadata, ByteBuffer[] addedCols,
      ByteBuffer[] addedVals) throws IOException, InterruptedException;

}