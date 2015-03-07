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

package org.apache.hadoop.hive.llap.io.metadata;

import java.util.List;

import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.FileMetadata;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Footer;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Metadata;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

/** ORC file metadata. Currently contains some duplicate info due to how different parts
 * of ORC use different info. Ideally we would get rid of protobuf structs in code beyond reading,
 * or instead use protobuf structs everywhere instead of the mix of things like now.
 */
public final class OrcFileMetadata extends LlapCacheableBuffer implements FileMetadata {
  private final long fileId;
  private final CompressionKind compressionKind;
  private final int compressionBufferSize;
  private final List<StripeInformation> stripes;
  private final boolean isOriginalFormat;
  private final List<Integer> versionList;
  private final int metadataSize;
  private final int writerVersionNum;
  private final Metadata metadata;
  private final Footer footer;

  public OrcFileMetadata(long fileId, Reader reader) {
    this.fileId = fileId;
    this.footer = reader.getFooterProto();
    this.metadata = reader.getMetadataProto();
    this.compressionKind = reader.getCompression();
    this.compressionBufferSize = reader.getCompressionSize();
    this.stripes = reader.getStripes(); // duplicates the footer
    this.isOriginalFormat = OrcInputFormat.isOriginal(reader); // duplicates the footer
    this.writerVersionNum = reader.getWriterVersion().getId();
    this.versionList = reader.getVersionList();
    this.metadataSize = reader.getMetadataSize();
  }

  // LlapCacheableBuffer
  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }

  @Override
  protected boolean invalidate() {
    return true; // relies on GC, so it can always be evicted now.
  }

  @Override
  public long getMemoryUsage() {
    // TODO#: add real estimate; we could do it almost entirely compile time (+list length),
    //        if it were not for protobufs. Get rid of protobufs here, or estimate them once?
    return 1024;
  }

  @Override
  protected boolean isLocked() {
    return false;
  }

  // FileMetadata
  @Override
  public List<OrcProto.Type> getTypes() {
    return footer.getTypesList();
  }

  @Override
  public boolean isOriginalFormat() {
    return isOriginalFormat;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return stripes;
  }

  @Override
  public CompressionKind getCompressionKind() {
    return compressionKind;
  }

  @Override
  public int getCompressionBufferSize() {
    return compressionBufferSize;
  }

  @Override
  public int getRowIndexStride() {
    return footer.getRowIndexStride();
  }

  @Override
  public int getColumnCount() {
    return footer.getTypesCount();
  }

  @Override
  public int getFlattenedColumnCount() {
    return footer.getTypes(0).getSubtypesCount();
  }

  @Override
  public long getFileId() {
    return fileId;
  }

  @Override
  public List<Integer> getVersionList() {
    return versionList;
  }

  @Override
  public int getMetadataSize() {
    return metadataSize;
  }

  @Override
  public int getWriterVersionNum() {
    return writerVersionNum;
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Footer getFooter() {
    return footer;
  }
}
