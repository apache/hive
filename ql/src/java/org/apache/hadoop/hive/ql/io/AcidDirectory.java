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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.AcidUtils.FileInfo;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hive.common.util.Ref;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.io.AcidUtils.AcidBaseFileType.ACID_SCHEMA;
import static org.apache.hadoop.hive.ql.io.AcidUtils.AcidBaseFileType.ORIGINAL_BASE;

/**
 * AcidDirectory used to provide ACID directory layout information, which directories and files to read.
 * This representation only valid in a context of a ValidWriteIdList and ValidTxnList.
 */
public final class AcidDirectory implements AcidUtils.Directory {

  private final Path path;
  private final FileSystem fs;
  private final Ref<Boolean> useFileId;

  private AcidUtils.ParsedBase base;
  private AcidUtils.ParsedBaseLight oldestBase;

  private final List<Path> abortedDirectories = new ArrayList<>();
  private final Set<Long> abortedWriteIds = new HashSet<>();
  private boolean unCompactedAborts;
  private final List<HadoopShims.HdfsFileStatusWithId> originalFiles = new ArrayList<>();
  private final List<Path> originalDirectories = new ArrayList<>();
  private final List<Path> obsolete = new ArrayList<>();
  private final List<AcidUtils.ParsedDelta> currentDirectories = new ArrayList<>();

  public AcidDirectory(Path path, FileSystem fs, Ref<Boolean> useFileId) {
    this.path = path;
    this.fs = fs;
    this.useFileId = useFileId;
    if (!(this.fs instanceof DistributedFileSystem) && this.useFileId != null) {
      this.useFileId.value = false;
    }
  }

  public Path getPath() {
    return path;
  }

  /**
   * Get the base directory path.
   * @return the base directory to read
   */
  public Path getBaseDirectory() {
    return base == null ? null : base.getBaseDirPath();
  }

  /**
   * Get the base directory.
   * @return the base directory to read
   */
  public AcidUtils.ParsedBase getBase() {
    return base;
  }

  /**
   * Oldest base directory in the filesystem, may be shadowed by newer base
   */
  public AcidUtils.ParsedBaseLight getOldestBase() {
    return oldestBase;
  }

  public void setBase(AcidUtils.ParsedBase base) {
    this.base = base;
  }

  public void setOldestBase(AcidUtils.ParsedBaseLight oldestBase) {
    this.oldestBase = oldestBase;
  }

  public void setUnCompactedAborts(boolean unCompactedAborts) {
    this.unCompactedAborts = unCompactedAborts;
  }

  /**
   * Is Base directory in raw format or in Acid format
   */
  public boolean isBaseInRawFormat() {
    return base != null && base.isRawFormat();
  }

  /**
   * Get the list of original files.  Not {@code null}.  Must be sorted.
   * @return the list of original files (eg. 000000_0)
   */
  public List<HadoopShims.HdfsFileStatusWithId> getOriginalFiles() {
    return originalFiles;
  }

  /**
   * List of original directories containing files in not ACID format
   */
  public List<Path> getOriginalDirectories() {
    return originalDirectories;
  }

  /**
   * Get the list of delta directories that are valid and not
   * obsolete.  Not {@code null}.  List must be sorted in a specific way.
   * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDeltaLight)}
   * for details.
   * @return the minimal list of current directories
   */
  public List<AcidUtils.ParsedDelta> getCurrentDirectories() {
    return currentDirectories;
  }

  /**
   * Get the list of obsolete directories. After filtering out bases and
   * deltas that are not selected by the valid transaction/write ids list, return the
   * list of original files, bases, and deltas that have been replaced by
   * more up to date ones.  Not {@code null}.
   */
  public List<Path> getObsolete() {
    return obsolete;
  }

  /**
   * Get the list of directories that has nothing but aborted transactions.
   * @return the list of aborted directories
   */
  public List<Path> getAbortedDirectories() {
    return abortedDirectories;
  }

  /**
   * Get the list of writeIds that belong to the aborted transactions.
   * @return the list of aborted writeIds
   */
  public Set<Long> getAbortedWriteIds() {
    return abortedWriteIds;
  }

  /**
   * Does the directory contain writeIds that belong to aborted transactions,
   * but are mixed together with committed writes. These aborted writes can not be cleaned.
   * @return true if there are aborted writes that can can be cleaned
   */
  public boolean hasUncompactedAborts() {
    return unCompactedAborts;
  }

  @Override
  public FileSystem getFs() {
    return fs;
  }

  /**
   * Delete deltas that should be read by this reader.
   */
  public List<AcidUtils.ParsedDelta> getDeleteDeltas() {
    return currentDirectories.stream().filter(AcidUtils.ParsedDeltaLight::isDeleteDelta).collect(Collectors.toList());
  }

  /**
   * All original, base and delta bucket files that should be read by this reader
   * @throws IOException ex
   */
  @Override
  public List<FileInfo> getFiles() throws IOException {
    List<FileInfo> baseAndDeltaFiles = new ArrayList<>();
    if (base == null) {
      // For non-acid tables (or paths), all data files are in getOriginalFiles() list
      for (HadoopShims.HdfsFileStatusWithId fileId : originalFiles) {
        baseAndDeltaFiles.add(new FileInfo(fileId, ORIGINAL_BASE));
      }
    } else {
      // The base files are either listed in ParsedBase or will be populated now
      for (HadoopShims.HdfsFileStatusWithId fileId : base.getFiles(fs, useFileId)) {
        baseAndDeltaFiles.add(new FileInfo(fileId,
            isBaseInRawFormat() ? ORIGINAL_BASE : ACID_SCHEMA));
      }
    }
    for (AcidUtils.ParsedDelta parsedDelta : currentDirectories) {
      if (parsedDelta.isDeleteDelta()) {
        continue;
      }
      if (parsedDelta.isRawFormat() && parsedDelta.getMinWriteId() != parsedDelta.getMaxWriteId()) {
        // delta/ with files in raw format are a result of Load Data (as opposed to compaction
        // or streaming ingest so must have interval length == 1.
        throw new IllegalStateException(
            "Delta in " + ORIGINAL_BASE + " format but txnIds are out of range: " + parsedDelta.getPath());
      }

      AcidUtils.AcidBaseFileType deltaType =
          parsedDelta.isRawFormat() ? ORIGINAL_BASE : ACID_SCHEMA;
      // The bucket files are either listed in ParsedDelta or will be populated now
      for (HadoopShims.HdfsFileStatusWithId deltaFile : parsedDelta.getFiles(fs, useFileId)) {
        baseAndDeltaFiles.add(new FileInfo(deltaFile, deltaType));
      }
    }

    return baseAndDeltaFiles;
  }

  @Override
  public String toString() {
    return "Aborted Directories: " + abortedDirectories + "; aborted writeIds: " + abortedWriteIds +
        "; original: " + originalFiles + "; obsolete: " + obsolete +
        "; currentDirectories: " + currentDirectories + "; base: " + base;
  }

}
