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
