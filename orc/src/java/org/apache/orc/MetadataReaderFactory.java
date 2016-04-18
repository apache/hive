package org.apache.orc;

import org.apache.orc.impl.MetadataReader;
import org.apache.orc.impl.MetadataReaderProperties;

import java.io.IOException;

public interface MetadataReaderFactory {

  MetadataReader create(MetadataReaderProperties properties) throws IOException;

}
