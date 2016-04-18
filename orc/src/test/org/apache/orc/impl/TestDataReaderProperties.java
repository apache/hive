package org.apache.orc.impl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestDataReaderProperties {

  private FileSystem mockedFileSystem = mock(FileSystem.class);
  private Path mockedPath = mock(Path.class);
  private CompressionCodec mockedCodec = mock(CompressionCodec.class);
  private boolean mockedZeroCopy = false;

  @Test
  public void testCompleteBuild() {
    DataReaderProperties properties = DataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withPath(mockedPath)
      .withCodec(mockedCodec)
      .withZeroCopy(mockedZeroCopy)
      .build();
    assertEquals(mockedFileSystem, properties.getFileSystem());
    assertEquals(mockedPath, properties.getPath());
    assertEquals(mockedCodec, properties.getCodec());
    assertEquals(mockedZeroCopy, properties.getZeroCopy());
  }

  @Test
  public void testMissingNonRequiredArgs() {
    DataReaderProperties properties = DataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withPath(mockedPath)
      .build();
    assertEquals(mockedFileSystem, properties.getFileSystem());
    assertEquals(mockedPath, properties.getPath());
    assertNull(properties.getCodec());
    assertFalse(properties.getZeroCopy());
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testEmptyBuild() {
    DataReaderProperties.builder().build();
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testMissingPath() {
    DataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withCodec(mockedCodec)
      .withZeroCopy(mockedZeroCopy)
      .build();
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testMissingFileSystem() {
    DataReaderProperties.builder()
      .withPath(mockedPath)
      .withCodec(mockedCodec)
      .withZeroCopy(mockedZeroCopy)
      .build();
  }

}
