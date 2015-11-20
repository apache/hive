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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

/**
 * Key for OrcFileMergeMapper task. Contains orc file related information that
 * should match before merging two orc files.
 */
public class OrcFileKeyWrapper implements WritableComparable<OrcFileKeyWrapper> {

  private Path inputPath;
  private CompressionKind compression;
  private int compressBufferSize;
  private List<OrcProto.Type> types;
  private int rowIndexStride;
  private OrcFile.Version version;
  private boolean isIncompatFile;

  public boolean isIncompatFile() {
    return isIncompatFile;
  }

  public void setIsIncompatFile(boolean isIncompatFile) {
    this.isIncompatFile = isIncompatFile;
  }

  public OrcFile.Version getVersion() {
    return version;
  }

  public void setVersion(OrcFile.Version version) {
    this.version = version;
  }

  public int getRowIndexStride() {
    return rowIndexStride;
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

  public List<OrcProto.Type> getTypes() {
    return types;
  }

  public void setTypes(List<OrcProto.Type> types) {
    this.types = types;
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
