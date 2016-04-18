package org.apache.orc.impl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestMetadataReaderProperties {

  private FileSystem mockedFileSystem = mock(FileSystem.class);
  private Path mockedPath = mock(Path.class);
  private CompressionCodec mockedCodec = mock(CompressionCodec.class);
  private int mockedBufferSize = 0;
  private int mockedTypeCount = 0;

  @Test
  public void testCompleteBuild() {
    MetadataReaderProperties properties = MetadataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withPath(mockedPath)
      .withCodec(mockedCodec)
      .withBufferSize(mockedBufferSize)
      .withTypeCount(mockedTypeCount)
      .build();
    assertEquals(mockedFileSystem, properties.getFileSystem());
    assertEquals(mockedPath, properties.getPath());
    assertEquals(mockedCodec, properties.getCodec());
    assertEquals(mockedBufferSize, properties.getBufferSize());
    assertEquals(mockedTypeCount, properties.getTypeCount());
  }

  @Test
  public void testMissingNonRequiredArgs() {
    MetadataReaderProperties properties = MetadataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withPath(mockedPath)
      .build();
    assertEquals(mockedFileSystem, properties.getFileSystem());
    assertEquals(mockedPath, properties.getPath());
    assertNull(properties.getCodec());
    assertEquals(0, properties.getBufferSize());
    assertEquals(0, properties.getTypeCount());
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testEmptyBuild() {
    MetadataReaderProperties.builder().build();
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testMissingPath() {
    MetadataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withCodec(mockedCodec)
      .withBufferSize(mockedBufferSize)
      .build();
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testMissingFileSystem() {
    MetadataReaderProperties.builder()
      .withPath(mockedPath)
      .withCodec(mockedCodec)
      .withBufferSize(mockedBufferSize)
      .build();
  }

}
