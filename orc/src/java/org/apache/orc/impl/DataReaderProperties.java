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
