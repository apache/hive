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

package org.apache.orc.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;

import javax.annotation.Nullable;

public final class DataReaderProperties {

  private final FileSystem fileSystem;
  private final Path path;
  private final CompressionCodec codec;
  private final boolean zeroCopy;

  private DataReaderProperties(Builder builder) {
    this.fileSystem = builder.fileSystem;
    this.path = builder.path;
    this.codec = builder.codec;
    this.zeroCopy = builder.zeroCopy;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public Path getPath() {
    return path;
  }

  @Nullable
  public CompressionCodec getCodec() {
    return codec;
  }

  public boolean getZeroCopy() {
    return zeroCopy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private FileSystem fileSystem;
    private Path path;
    private CompressionCodec codec;
    private boolean zeroCopy;

    private Builder() {

    }

    public Builder withFileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder withPath(Path path) {
      this.path = path;
      return this;
    }

    public Builder withCodec(CompressionCodec codec) {
      this.codec = codec;
      return this;
    }

    public Builder withZeroCopy(boolean zeroCopy) {
      this.zeroCopy = zeroCopy;
      return this;
    }

    public DataReaderProperties build() {
      Preconditions.checkNotNull(fileSystem);
      Preconditions.checkNotNull(path);

      return new DataReaderProperties(this);
    }

  }
}
