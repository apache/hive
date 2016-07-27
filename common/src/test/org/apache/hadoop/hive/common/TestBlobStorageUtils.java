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

package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.hive.common.BlobStorageUtils.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestBlobStorageUtils {
  private static final Configuration conf = new Configuration();

  @Before
  public void setUp() {
    conf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname, "s3a,swift");
    conf.setBoolean(HiveConf.ConfVars.HIVE_BLOBSTORE_USE_BLOBSTORE_AS_SCRATCHDIR.varname, false);
  }

  @Test
  public void testValidAndInvalidPaths() throws IOException {
    // Valid paths
    assertTrue(isBlobStoragePath(conf, new Path("s3a://bucket/path")));
    assertTrue(isBlobStoragePath(conf, new Path("swift://bucket/path")));

    // Invalid paths
    assertFalse(isBlobStoragePath(conf, new Path("/tmp/a-path")));
    assertFalse(isBlobStoragePath(conf, new Path("s3fs://tmp/file")));
    assertFalse(isBlobStoragePath(conf, null));
    assertFalse(isBlobStorageFileSystem(conf, null));
    assertFalse(isBlobStoragePath(conf, new Path(URI.create(""))));
  }

  @Test
  public void testValidAndInvalidFileSystems() {
    FileSystem fs = mock(FileSystem.class);

    /* Valid FileSystem schemes */

    doReturn("s3a").when(fs).getScheme();
    assertTrue(isBlobStorageFileSystem(conf, fs));

    doReturn("swift").when(fs).getScheme();
    assertTrue(isBlobStorageFileSystem(conf, fs));

    /* Invalid FileSystem schemes */

    doReturn("hdfs").when(fs).getScheme();
    assertFalse(isBlobStorageFileSystem(conf, fs));

    doReturn("").when(fs).getScheme();
    assertFalse(isBlobStorageFileSystem(conf, fs));

    assertFalse(isBlobStorageFileSystem(conf, null));
  }

  @Test
  public void testValidAndInvalidSchemes() {
    // Valid schemes
    assertTrue(isBlobStorageScheme(conf, "s3a"));
    assertTrue(isBlobStorageScheme(conf, "swift"));

    // Invalid schemes
    assertFalse(isBlobStorageScheme(conf, "hdfs"));
    assertFalse(isBlobStorageScheme(conf, ""));
    assertFalse(isBlobStorageScheme(conf, null));
  }
}
