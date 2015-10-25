/**
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

package org.apache.hadoop.hive.ql.io.orc;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterVersion;

/**
 * FileMetaInfo - represents file metadata stored in footer and postscript sections of the file
 * that is useful for Reader implementation
 *
 */
public class FileMetaInfo {
  ByteBuffer footerMetaAndPsBuffer;
  final String compressionType;
  final int bufferSize;
  final int metadataSize;
  final ByteBuffer footerBuffer;
  final List<Integer> versionList;
  final OrcFile.WriterVersion writerVersion;


  /** Ctor used when reading splits - no version list or full footer buffer. */
  FileMetaInfo(String compressionType, int bufferSize, int metadataSize,
      ByteBuffer footerBuffer, OrcFile.WriterVersion writerVersion) {
    this(compressionType, bufferSize, metadataSize, footerBuffer, null,
        writerVersion, null);
  }

  /** Ctor used when creating file info during init and when getting a new one. */
  public FileMetaInfo(String compressionType, int bufferSize, int metadataSize,
      ByteBuffer footerBuffer, List<Integer> versionList, WriterVersion writerVersion,
      ByteBuffer fullFooterBuffer) {
    this.compressionType = compressionType;
    this.bufferSize = bufferSize;
    this.metadataSize = metadataSize;
    this.footerBuffer = footerBuffer;
    this.versionList = versionList;
    this.writerVersion = writerVersion;
    this.footerMetaAndPsBuffer = fullFooterBuffer;
  }

  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }
}