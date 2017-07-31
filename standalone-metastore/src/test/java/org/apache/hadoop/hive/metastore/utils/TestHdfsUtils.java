/*
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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Random;

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

  private Configuration makeConf() {
    // Make sure that the user doesn't happen to be in the super group
    Configuration conf = new Configuration();
    conf.set("dfs.permissions.supergroup", "ubermensch");
    return conf;
  }

  private UserGroupInformation ugiInvalidUserValidGroups() throws LoginException, IOException {
    UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(ugi.getShortUserName()).thenReturn("nosuchuser");
    Mockito.when(ugi.getGroupNames()).thenReturn(SecurityUtils.getUGI().getGroupNames());
    return ugi;
  }

  private UserGroupInformation ugiInvalidUserInvalidGroups() {
    UserGroupInformation ugi = Mockito.mock(UserGroupInformation.class);
    Mockito.when(ugi.getShortUserName()).thenReturn("nosuchuser");
    Mockito.when(ugi.getGroupNames()).thenReturn(new String[]{"nosuchgroup"});
    return ugi;
  }

  @Test
  public void userReadWriteExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoRead() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoWrite() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void userNoExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.ALL));
    UserGroupInformation ugi = SecurityUtils.getUGI();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test
  public void groupReadWriteExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoRead() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoWrite() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void groupNoExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserValidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test
  public void otherReadWriteExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.ALL));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoRead() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoWrite() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
  }

  @Test(expected = AccessControlException.class)
  public void otherNoExecute() throws IOException, LoginException {
    FileSystem fs = FileSystem.get(makeConf());
    Path p = createFile(fs, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    UserGroupInformation ugi = ugiInvalidUserInvalidGroups();
    HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
  }

  @Test
  public void rootReadWriteExecute() throws IOException, LoginException {
    UserGroupInformation ugi = SecurityUtils.getUGI();
    FileSystem fs = FileSystem.get(new Configuration());
    String old = fs.getConf().get("dfs.permissions.supergroup");
    try {
      fs.getConf().set("dfs.permissions.supergroup", ugi.getPrimaryGroupName());
      Path p = createFile(fs, new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));
      HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.READ, ugi);
      HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.WRITE, ugi);
      HdfsUtils.checkFileAccess(fs, fs.getFileStatus(p), FsAction.EXECUTE, ugi);
    } finally {
      fs.getConf().set("dfs.permissions.supergroup", old);
    }
  }

}
