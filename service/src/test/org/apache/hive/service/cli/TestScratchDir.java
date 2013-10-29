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

package org.apache.hive.service.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.Test;

public class TestScratchDir {
  @Test
  public void testScratchDirs() throws Exception {
    stageDirTest("hive.exec.scratchdir", "TestScratchDirs_foobar", false);
  }

  @Test
  public void testLocalScratchDirs() throws Exception {
    stageDirTest("hive.exec.local.scratchdir", "TestLocalScratchDirs_foobar", true);
  }

  @Test
  public void testResourceDirs() throws Exception {
    stageDirTest("hive.downloaded.resources.dir", "TestResourceDirs_foobar", true);
  }

  private void stageDirTest(String stageDirConfigStr, String stageDirName, boolean isLocal) throws IOException {
    String scratchDirStr = System.getProperty("test.tmp.dir") + File.separator +
        stageDirName;
    System.setProperty(stageDirConfigStr, scratchDirStr);
    ThriftCLIService service = new EmbeddedThriftBinaryCLIService();
    ThriftCLIServiceClient client = new ThriftCLIServiceClient(service);
    final Path scratchDir = new Path(scratchDirStr);
    Configuration conf = new Configuration();
    FileSystem fs = scratchDir.getFileSystem(conf);
    if (isLocal) {
      fs = FileSystem.getLocal(conf);
    }
    assertTrue(fs.exists(scratchDir));

    FileStatus[] fStatus = fs.globStatus(scratchDir);
    boolean foo = fStatus[0].equals(new FsPermission((short)0777));
    assertEquals(new FsPermission((short)0777), fStatus[0].getPermission());
    service.stop();
    fs.delete(scratchDir, true);
    System.clearProperty(stageDirConfigStr);
  }

}
