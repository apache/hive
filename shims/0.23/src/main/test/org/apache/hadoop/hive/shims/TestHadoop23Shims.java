/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHadoop23Shims {

  /**
   * Tests that {@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not throw an exception when setting the group and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritGroup() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(HadoopShims.HdfsFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FileSystem fs = mock(FileSystem.class);

    when(mockSourceStatus.getGroup()).thenReturn("fakeGroup1");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(fs).setOwner(any(Path.class), any(String.class), any(String.class));

    ShimLoader.getHadoopShims().setFullFileStatus(conf, mockHadoopFileStatus, "fakeGroup2", fs, new Path("fakePath"),
            false);
    verify(fs).setOwner(any(Path.class), any(String.class), any(String.class));
  }

  /**
   * Tests that {{@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not thrown an exception when setting ACLs and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritAcls() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "true");

    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(Hadoop23Shims.Hadoop23FileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    AclStatus mockAclStatus = mock(AclStatus.class);
    FileSystem mockFs = mock(FileSystem.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockAclStatus.toString()).thenReturn("");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    when(((Hadoop23Shims.Hadoop23FileStatus) mockHadoopFileStatus).getAclEntries()).thenReturn(
            new ArrayList<AclEntry>());
    when(((Hadoop23Shims.Hadoop23FileStatus) mockHadoopFileStatus).getAclStatus()).thenReturn(mockAclStatus);
    doThrow(RuntimeException.class).when(mockFs).setAcl(any(Path.class), any(List.class));

    new Hadoop23Shims().setFullFileStatus(conf, mockHadoopFileStatus, null, mockFs, new Path("fakePath"),
            false);
    verify(mockFs).setAcl(any(Path.class), any(List.class));
  }

  /**
   * Tests that {@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not thrown an exception when setting permissions and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritPerms() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(HadoopShims.HdfsFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FileSystem mockFs = mock(FileSystem.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFs).setPermission(any(Path.class), any(FsPermission.class));

    ShimLoader.getHadoopShims().setFullFileStatus(conf, mockHadoopFileStatus, null, mockFs, new Path("fakePath"),
            false);
    verify(mockFs).setPermission(any(Path.class), any(FsPermission.class));
  }

  /**
   * Tests that {@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not throw an exception when setting the group and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritGroupRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    String fakeSourceGroup = "fakeGroup1";
    String fakeTargetGroup = "fakeGroup2";
    Path fakeTarget = new Path("fakePath");
    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(HadoopShims.HdfsFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);

    when(mockSourceStatus.getGroup()).thenReturn(fakeSourceGroup);
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    ShimLoader.getHadoopShims().setFullFileStatus(conf, mockHadoopFileStatus, fakeTargetGroup, mock(FileSystem.class),
            fakeTarget,
            true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-chgrp", "-R", fakeSourceGroup, fakeTarget.toString()});
  }

  /**
   * Tests that {@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not thrown an exception when setting ACLs and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritAclsRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "true");

    Path fakeTarget = new Path("fakePath");
    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(Hadoop23Shims.Hadoop23FileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);
    AclStatus mockAclStatus = mock(AclStatus.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockAclStatus.toString()).thenReturn("");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    when(((Hadoop23Shims.Hadoop23FileStatus) mockHadoopFileStatus).getAclEntries()).thenReturn(
            new ArrayList<AclEntry>());
    when(((Hadoop23Shims.Hadoop23FileStatus) mockHadoopFileStatus).getAclStatus()).thenReturn(mockAclStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    new Hadoop23Shims().setFullFileStatus(conf, mockHadoopFileStatus, "", mock(FileSystem.class), fakeTarget,
            true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-setfacl", "-R", "--set", any(String.class), fakeTarget.toString()});
  }

  /**
   * Tests that {@link org.apache.hadoop.hive.shims.HadoopShims#setFullFileStatus(Configuration, HadoopShims.HdfsFileStatus, String, FileSystem, Path, boolean)}
   * does not thrown an exception when setting permissions and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritPermsRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    Path fakeTarget = new Path("fakePath");
    HadoopShims.HdfsFileStatus mockHadoopFileStatus = mock(HadoopShims.HdfsFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    ShimLoader.getHadoopShims().setFullFileStatus(conf, mockHadoopFileStatus, "", mock(FileSystem.class), fakeTarget,
            true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-chmod", "-R", any(String.class), fakeTarget.toString()});
  }
}
