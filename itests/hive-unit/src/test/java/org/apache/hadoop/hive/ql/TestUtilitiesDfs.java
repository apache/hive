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

package org.apache.hadoop.hive.ql;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestUtilitiesDfs {
  private static final FsPermission FULL_PERM = new FsPermission((short) 00777);
  private static MiniDFSShim dfs;
  private static HiveConf conf;

  @BeforeClass
  public static void setupDfs() throws Exception {
    conf = new HiveConf();
    dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);

    assertNotNull("MiniDFS is not initialized", dfs);
    assertNotNull("HiveConf is not initialized", conf);
  }

  @Test
  public void testCreateDirWithPermissionRecursive() throws IllegalArgumentException, IOException {
    FileSystem fs = dfs.getFileSystem();
    Path dir = new Path(new Path(fs.getUri()), "/testUtilitiesUMaskReset");
    Utilities.createDirsWithPermission(conf, dir, FULL_PERM, true);
    FileStatus status = fs.getFileStatus(dir);
    assertEquals("Created dir has invalid permissions.",
        FULL_PERM.toString(), status.getPermission().toString());
  }

  @AfterClass
  public static void shutdownDfs() throws Exception {
    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }
}
