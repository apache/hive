package org.apache.orc.impl;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestDataReaderProperties {

  private FileSystem mockedFileSystem = mock(FileSystem.class);
  private Path mockedPath = mock(Path.class);
  private boolean mockedZeroCopy = false;

  @Test
  public void testCompleteBuild() {
    DataReaderProperties properties = DataReaderProperties.builder()
      .withFileSystem(mockedFileSystem)
      .withPath(mockedPath)
      .withCompression(CompressionKind.ZLIB)
      .withZeroCopy(mockedZeroCopy)
      .build();
    assertEquals(mockedFileSystem, properties.getFileSystem());
    assertEquals(mockedPath, properties.getPath());
    assertEquals(CompressionKind.ZLIB, properties.getCompression());
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
    assertNull(properties.getCompression());
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
      .withCompression(CompressionKind.NONE)
      .withZeroCopy(mockedZeroCopy)
      .build();
  }

  @Test(expected = java.lang.NullPointerException.class)
  public void testMissingFileSystem() {
    DataReaderProperties.builder()
      .withPath(mockedPath)
      .withCompression(CompressionKind.NONE)
      .withZeroCopy(mockedZeroCopy)
      .build();
  }

}
