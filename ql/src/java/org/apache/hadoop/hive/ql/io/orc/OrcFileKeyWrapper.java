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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;

/**
 * Key for OrcFileMergeMapper task. Contains orc file related information that
 * should match before merging two orc files.
 */
public class OrcFileKeyWrapper implements WritableComparable<OrcFileKeyWrapper> {

  private Path inputPath;
  private CompressionKind compression;
  private int compressBufferSize;
  private TypeDescription fileSchema;
  private int rowIndexStride;
  private OrcFile.Version fileVersion;
  private OrcFile.WriterVersion writerVersion;
  private boolean isIncompatFile;

  public boolean isIncompatFile() {
    return isIncompatFile;
  }

  public void setIsIncompatFile(boolean isIncompatFile) {
    this.isIncompatFile = isIncompatFile;
  }

  public int getRowIndexStride() {
    return rowIndexStride;
  }

  public OrcFile.Version getFileVersion() {
    return fileVersion;
  }

  public void setFileVersion(final OrcFile.Version fileVersion) {
    this.fileVersion = fileVersion;
  }

  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }

  public void setWriterVersion(final OrcFile.WriterVersion writerVersion) {
    this.writerVersion = writerVersion;
  }

  public void setRowIndexStride(int rowIndexStride) {
    this.rowIndexStride = rowIndexStride;
  }

  public int getCompressBufferSize() {
    return compressBufferSize;
  }

  public void setCompressBufferSize(int compressBufferSize) {
    this.compressBufferSize = compressBufferSize;
  }

  public CompressionKind getCompression() {
    return compression;
  }

  public void setCompression(CompressionKind compression) {
    this.compression = compression;
  }

  public TypeDescription getFileSchema() {
    return fileSchema;
  }

  public void setFileSchema(final TypeDescription fileSchema) {
    this.fileSchema = fileSchema;
  }

  public Path getInputPath() {
    return inputPath;
  }

  public void setInputPath(Path inputPath) {
    this.inputPath = inputPath;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public int compareTo(OrcFileKeyWrapper o) {
    return inputPath.compareTo(o.inputPath);
  }

}
