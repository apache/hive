/*
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
package org.apache.hive.service.server;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.Assert;
import org.junit.Test;

public class TestHS2ClearDanglingScratchDir {
  @Test
  public void testScratchDirCleared() throws Exception {
    MiniDFSCluster m_dfs = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    HiveConf conf = new HiveConf();
    conf.addResource(m_dfs.getConfiguration(0));
    conf.set(HiveConf.ConfVars.HIVE_SCRATCH_DIR_LOCK.toString(), "true");
    conf.set(HiveConf.ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR.toString(), "true");

    Path scratchDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCH_DIR));
    m_dfs.getFileSystem().mkdirs(scratchDir);
    m_dfs.getFileSystem().setPermission(scratchDir, new FsPermission("777"));

    // Fake two live session
    SessionState.start(conf);
    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_ID, UUID.randomUUID().toString());
    SessionState.start(conf);

    // Fake dead session
    Path fakeSessionPath = new Path(new Path(scratchDir, Utils.getUGI().getShortUserName()),
        UUID.randomUUID().toString());
    m_dfs.getFileSystem().mkdirs(fakeSessionPath);
    m_dfs.getFileSystem().create(new Path(fakeSessionPath, "inuse.lck")).close();

    FileStatus[] scratchDirs = m_dfs.getFileSystem()
        .listStatus(new Path(scratchDir, Utils.getUGI().getShortUserName()));

    Assert.assertEquals(scratchDirs.length, 3);

    HiveServer2.scheduleClearDanglingScratchDir(conf, 0);

    // Check dead session get cleared
    long start = System.currentTimeMillis();
    long end;
    do {
      Thread.sleep(200);
      end = System.currentTimeMillis();
      if (end - start > 5000) {
        Assert.fail("timeout, scratch dir has not been cleared");
      }
      scratchDirs = m_dfs.getFileSystem()
          .listStatus(new Path(scratchDir, Utils.getUGI().getShortUserName()));
    } while (scratchDirs.length != 2);
  }
}
