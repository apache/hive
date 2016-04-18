package org.apache.orc.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;

import javax.annotation.Nullable;

public final class MetadataReaderProperties {

  private final FileSystem fileSystem;
  private final Path path;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final int typeCount;

  private MetadataReaderProperties(Builder builder) {
    this.fileSystem = builder.fileSystem;
    this.path = builder.path;
    this.codec = builder.codec;
    this.bufferSize = builder.bufferSize;
    this.typeCount = builder.typeCount;
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

  public int getBufferSize() {
    return bufferSize;
  }

  public int getTypeCount() {
    return typeCount;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private FileSystem fileSystem;
    private Path path;
    private CompressionCodec codec;
    private int bufferSize;
    private int typeCount;

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

    public Builder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder withTypeCount(int typeCount) {
      this.typeCount = typeCount;
      return this;
    }

    public MetadataReaderProperties build() {
      Preconditions.checkNotNull(fileSystem);
      Preconditions.checkNotNull(path);

      return new MetadataReaderProperties(this);
    }

  }
}
