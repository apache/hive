package org.apache.orc.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;

import javax.annotation.Nullable;

public final class DataReaderProperties {

  private final FileSystem fileSystem;
  private final Path path;
  private final CompressionKind compression;
  private final boolean zeroCopy;
  private final int typeCount;
  private final int bufferSize;

  private DataReaderProperties(Builder builder) {
    this.fileSystem = builder.fileSystem;
    this.path = builder.path;
    this.compression = builder.compression;
    this.zeroCopy = builder.zeroCopy;
    this.typeCount = builder.typeCount;
    this.bufferSize = builder.bufferSize;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public Path getPath() {
    return path;
  }

  public CompressionKind getCompression() {
    return compression;
  }

  public boolean getZeroCopy() {
    return zeroCopy;
  }

  public int getTypeCount() {
    return typeCount;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private FileSystem fileSystem;
    private Path path;
    private CompressionKind compression;
    private boolean zeroCopy;
    private int typeCount;
    private int bufferSize;

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

    public Builder withCompression(CompressionKind value) {
      this.compression = value;
      return this;
    }

    public Builder withZeroCopy(boolean zeroCopy) {
      this.zeroCopy = zeroCopy;
      return this;
    }

    public Builder withTypeCount(int value) {
      this.typeCount = value;
      return this;
    }

    public Builder withBufferSize(int value) {
      this.bufferSize = value;
      return this;
    }

    public DataReaderProperties build() {
      Preconditions.checkNotNull(fileSystem);
      Preconditions.checkNotNull(path);

      return new DataReaderProperties(this);
    }

  }
}
