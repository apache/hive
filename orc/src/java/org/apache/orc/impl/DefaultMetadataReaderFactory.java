package org.apache.orc.impl;

import org.apache.orc.MetadataReaderFactory;

import java.io.IOException;

public final class DefaultMetadataReaderFactory implements MetadataReaderFactory {

  @Override
  public MetadataReader create(MetadataReaderProperties properties) throws IOException {
    return new MetadataReaderImpl(properties);
  }

}
