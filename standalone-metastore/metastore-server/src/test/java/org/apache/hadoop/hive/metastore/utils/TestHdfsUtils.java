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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestHdfsUtils {

  private Random rand = new Random();

  private Path createFile(FileSystem fs, FsPermission perms) throws IOException {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      buf.append((char)(rand.nextInt(26) + 'a'));
    }
    Path p = new Path(buf.toString());
    FSDataOutputStream os = fs.create(p);
    os.writeBytes("Mary had a little lamb\nit's fleece was white as snow\nand anywhere that Mary " +
        "went\nthe lamb was sure to go\n");
    os.close();
    fs.setPermission(p, perms);
    fs.deleteOnExit(p);
    return p;
  }

  private UserGroupInformation ugiInvalidUserValidGroups() throws IOException {
    return UserGroupInformation.createUserForTesting("nosuchuser", SecurityUtils.getUGI().getGroupNames());
  }

  private UserGroupInformation ugiInvalidUserInvalidGroups() {
    return UserGroupInformation.createUserForTesting("nosuchuser", new String[]{"nosuchgroup"});
  }

  @Test
  public void userReadWriteExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoRead() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoWrite() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test
  public void groupReadWriteExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoRead() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoWrite() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test
  public void otherReadWriteExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoRead() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoWrite() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoExecute() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  @Test (expected = FileNotFoundException.class)
  public void nonExistentFile() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path p = new Path("/tmp/nosuchfile");
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, p, FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void accessPerssionFromCustomFilesystem() throws IOException {
    FileSystem fs = new RawLocalFileSystem() {
      @Override
      public void access(Path path, FsAction mode) throws AccessControlException, IOException {
        if (path.toString().contains("noaccess")) {
          throw new AccessControlException("no access");
        }
      }

      @Override
      public Configuration getConf() {
        return new Configuration();
      }
    };

    UserGroupInformation ugi = SecurityUtils.getUGI();
    Path p = new Path("/tmp/noaccess");
    HdfsUtils.checkFileAccess(fs, p, FsAction.EXECUTE, ugi);
  }

  /**
   * Tests that {@link HdfsUtils#setFullFileStatus(Configuration, HdfsUtils.HadoopFileStatus, String, FileSystem, Path, boolean)}
   * does not throw an exception when setting the group and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritGroup() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FileSystem fs = mock(FileSystem.class);

    when(mockSourceStatus.getGroup()).thenReturn("fakeGroup1");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(fs).setOwner(any(Path.class), any(String.class), any(String.class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, "fakeGroup2", fs, new Path("fakePath"), false);
    verify(fs).setOwner(any(Path.class), any(), any(String.class));
  }

  /**
   * Tests that link HdfsUtils#setFullFileStatus
   * does not thrown an exception when setting ACLs and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritAcls() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "true");

    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    AclStatus mockAclStatus = mock(AclStatus.class);
    FileSystem mockFs = mock(FileSystem.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockAclStatus.toString()).thenReturn("");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    when(mockHadoopFileStatus.getAclEntries()).thenReturn(new ArrayList<>());
    when(mockHadoopFileStatus.getAclStatus()).thenReturn(mockAclStatus);
    doThrow(RuntimeException.class).when(mockFs).setAcl(any(Path.class), any(List.class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, null, mockFs, new Path("fakePath"), false);
    verify(mockFs).setAcl(any(Path.class), any(List.class));
  }

  /**
   * Tests that HdfsUtils#setFullFileStatus
   * does not thrown an exception when setting permissions and without recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritPerms() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FileSystem mockFs = mock(FileSystem.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFs).setPermission(any(Path.class), any(FsPermission.class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, null, mockFs, new Path("fakePath"),
        false);
    verify(mockFs).setPermission(any(Path.class), any(FsPermission.class));
  }

  /**
   * Tests that {@link HdfsUtils#setFullFileStatus(Configuration, HdfsUtils.HadoopFileStatus, String, FileSystem, Path, boolean)}
   * does not throw an exception when setting the group and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritGroupRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    String fakeSourceGroup = "fakeGroup1";
    String fakeTargetGroup = "fakeGroup2";
    Path fakeTarget = new Path("fakePath");
    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);

    when(mockSourceStatus.getGroup()).thenReturn(fakeSourceGroup);
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, fakeTargetGroup, mock(FileSystem.class), fakeTarget,
        true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-chgrp", "-R", fakeSourceGroup, fakeTarget.toString()});
  }

  /**
   * Tests that HdfsUtils#setFullFileStatus
   * does not thrown an exception when setting ACLs and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritAclsRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "true");

    Path fakeTarget = new Path("fakePath");
    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);
    AclStatus mockAclStatus = mock(AclStatus.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockAclStatus.toString()).thenReturn("");
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    when(mockHadoopFileStatus.getAclEntries()).thenReturn(new ArrayList<>());
    when(mockHadoopFileStatus.getAclStatus()).thenReturn(mockAclStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, "", mock(FileSystem.class), fakeTarget, true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-setfacl", "-R", "--set", any(), fakeTarget.toString()});
  }

  /**
   * Tests that HdfsUtils#setFullFileStatus
   * does not thrown an exception when setting permissions and with recursion.
   */
  @Test
  public void testSetFullFileStatusFailInheritPermsRecursive() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.acls.enabled", "false");

    Path fakeTarget = new Path("fakePath");
    HdfsUtils.HadoopFileStatus mockHadoopFileStatus = mock(HdfsUtils.HadoopFileStatus.class);
    FileStatus mockSourceStatus = mock(FileStatus.class);
    FsShell mockFsShell = mock(FsShell.class);

    when(mockSourceStatus.getPermission()).thenReturn(new FsPermission((short) 777));
    when(mockHadoopFileStatus.getFileStatus()).thenReturn(mockSourceStatus);
    doThrow(RuntimeException.class).when(mockFsShell).run(any(String[].class));

    HdfsUtils.setFullFileStatus(conf, mockHadoopFileStatus, "", mock(FileSystem.class), fakeTarget,
        true, mockFsShell);
    verify(mockFsShell).run(new String[]{"-chmod", "-R", any(), fakeTarget.toString()});
  }
}
