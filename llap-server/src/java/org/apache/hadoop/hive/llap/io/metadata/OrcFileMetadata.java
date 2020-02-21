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

package org.apache.hadoop.hive.llap.io.metadata;

import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.orc.CompressionKind;
import org.apache.orc.FileFormatException;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.CalendarKind;
import org.apache.orc.OrcProto.StripeStatistics;
import org.apache.orc.OrcUtils;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

/** ORC file metadata. Currently contains some duplicate info due to how different parts
 * of ORC use different info. Ideally we would get rid of protobuf structs in code beyond reading,
 * or instead use protobuf structs everywhere instead of the mix of things like now.
 */
public final class OrcFileMetadata implements FileMetadata, ConsumerFileMetadata {
  private final List<StripeInformation> stripes;
  private final List<Integer> versionList;
  private final List<OrcProto.StripeStatistics> stripeStats;
  private final List<OrcProto.Type> types;
  private final List<OrcProto.ColumnStatistics> fileStats;
  private final Object fileKey;
  private final CompressionKind compressionKind;
  private final int rowIndexStride;
  private final int compressionBufferSize;
  private final int metadataSize;
  private final int writerVersionNum;
  private final long contentLength;
  private final long numberOfRows;
  private final boolean isOriginalFormat;
  private final OrcFile.Version fileVersion;
  private final CalendarKind calendar;

  public OrcFileMetadata(Object fileKey, OrcProto.Footer footer, OrcProto.PostScript ps,
    List<StripeStatistics> stats, List<StripeInformation> stripes, final OrcFile.Version fileVersion) {
    this.stripeStats = stats;
    this.compressionKind = CompressionKind.valueOf(ps.getCompression().name());
    this.compressionBufferSize = (int)ps.getCompressionBlockSize();
    this.stripes = stripes;
    this.isOriginalFormat = OrcInputFormat.isOriginal(footer);
    this.writerVersionNum = ps.getWriterVersion();
    this.versionList = ps.getVersionList();
    this.metadataSize = (int) ps.getMetadataLength();
    this.types = footer.getTypesList();
    this.rowIndexStride = footer.getRowIndexStride();
    this.contentLength = footer.getContentLength();
    this.numberOfRows = footer.getNumberOfRows();
    this.fileStats = footer.getStatisticsList();
    this.fileKey = fileKey;
    this.fileVersion = fileVersion;
    this.calendar = footer.getCalendar();
  }

  // FileMetadata
  @Override
  public List<OrcProto.Type> getTypes() {
    return types;
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
    return rowIndexStride;
  }

  @Override
  public int getColumnCount() {
    return types.size();
  }

  @Override
  public int getFlattenedColumnCount() {
    return types.get(0).getSubtypesCount();
  }

  @Override
  public Object getFileKey() {
    return fileKey;
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
  public int getWriterImplementation() {
    return OrcFile.WriterImplementation.ORC_JAVA.getId();
  }

  @Override
  public int getWriterVersionNum() {
    return writerVersionNum;
  }

  @Override
  public List<OrcProto.StripeStatistics> getStripeStats() {
    return stripeStats;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public long getNumberOfRows() {
    return numberOfRows;
  }

  @Override
  public List<OrcProto.ColumnStatistics> getFileStats() {
    return fileStats;
  }

  @Override
  public int getStripeCount() {
    return stripes.size();
  }

  public TypeDescription getSchema() throws FileFormatException {
    return OrcUtils.convertTypeFromProtobuf(this.types, 0);
  }

  @Override
  public OrcFile.Version getFileVersion() {
    return fileVersion;
  }

  @Override
  public CalendarKind getCalendar() {
    return calendar;
  }
}
