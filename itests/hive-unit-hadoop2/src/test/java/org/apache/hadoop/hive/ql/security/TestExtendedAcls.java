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
package org.apache.hadoop.hive.ql.security;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.MASK;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestExtendedAcls extends FolderPermissionBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestExtendedAcls.class);

  @BeforeClass
  public static void setup() throws Exception {
    conf = new HiveConf(TestExtendedAcls.class);
    //setup the mini DFS with acl's enabled.
    conf.set("dfs.namenode.acls.enabled", "true");
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    baseSetup();
  }

  // ACLs for setting up directories and files.
  private final ImmutableList<AclEntry> aclsForDirs1 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.ALL),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL));

  private final ImmutableList<AclEntry> aclsForDirs2 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL));

  private final ImmutableList<AclEntry> aclsForFiles1 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.ALL),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE));

  private final ImmutableList<AclEntry> aclsForFiles2 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, MASK, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE));

  // ACLs for verifying directories and files.
  private final ImmutableList<AclEntry> aclDirSpec1 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.ALL),
      aclEntry(DEFAULT, OTHER, FsAction.ALL),
      aclEntry(DEFAULT, USER, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL));

  private final ImmutableList<AclEntry> aclDirSpec2 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.ALL),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL));

  private final FsPermission perm1 = new FsPermission((short) 0777);
  private final FsPermission perm2 = new FsPermission((short) 0775);

  private final ImmutableList<AclEntry> aclFileSpec1 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE));

  private final ImmutableList<AclEntry> aclFileSpec2 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE));

  private final FsPermission permFile1 = new FsPermission((short) 0770);
  private final FsPermission permFile2 = new FsPermission((short) 0770);

  private final ImmutableList<AclEntry> aclInheritedSpec1 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.ALL),
      aclEntry(DEFAULT, OTHER, FsAction.ALL),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo", FsAction.READ_EXECUTE));

  private final ImmutableList<AclEntry> aclInheritedSpec2 = ImmutableList.of(
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, USER, FsAction.ALL),
      aclEntry(DEFAULT, GROUP, FsAction.ALL),
      aclEntry(DEFAULT, OTHER, FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, MASK, FsAction.ALL),
      aclEntry(DEFAULT, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, "bar2", FsAction.READ_WRITE),
      aclEntry(DEFAULT, GROUP, "foo2", FsAction.READ_EXECUTE));

  private final FsPermission permInherited1 = new FsPermission((short) 0770);
  private final FsPermission permInherited2 = new FsPermission((short) 0770);

  @Override
  public void setPermission(String locn, int permIndex) throws Exception {
    Path path = new Path(locn);
    FileStatus fstat = fs.getFileStatus(path);

    switch (permIndex) {
      case 0:
        fs.setAcl(path, fstat.isDir() ? aclsForDirs1 : aclsForFiles1);
        break;
      case 1:
        fs.setAcl(path, fstat.isDir() ? aclsForDirs2 : aclsForFiles2);
        break;
      default:
        throw new RuntimeException("Only 2 permissions by this test");
    }
  }

  @Override
  public void verifyPermission(String locn, int permIndex) throws Exception {
    Path path = new Path(locn);
    FileStatus fstat = fs.getFileStatus(path);
    FsPermission perm = fstat.getPermission();
    List<AclEntry> actual = null;

    switch (permIndex) {
      case 0:
        actual = fs.getAclStatus(path).getEntries();
        verify(path, perm1, perm, aclDirSpec1, actual);
        break;
      case 1:
        actual = fs.getAclStatus(path).getEntries();
        verify(path, perm2, perm, aclDirSpec2, actual);
        break;
      default:
        throw new RuntimeException("Only 2 permissions by this test: " + permIndex);
    }
  }

  @Override
  public void verifyInheritedPermission(String locn, int permIndex) throws Exception {
    Path path = new Path(locn);
    FileStatus fstat = fs.getFileStatus(path);
    FsPermission perm = fstat.getPermission();
    List<AclEntry> acls = fs.getAclStatus(path).getEntries();
    FsPermission expectedPerm = null;
    List<AclEntry> expectedAcls = null;

    switch (permIndex) {
      case 0:
        //Assert.assertEquals("Location: " + locn, "rwxrwxrwx", String.valueOf(perm));

        expectedPerm = fstat.isFile() ? permFile1 : permInherited1;
        expectedAcls = fstat.isFile() ? aclFileSpec1 : aclInheritedSpec1;

        verify(path, expectedPerm, perm, expectedAcls, acls);
        break;
      case 1:
        //Assert.assertEquals("Location: " + locn, "rwxrwxr-x", String.valueOf(perm));

        expectedPerm = fstat.isFile() ? permFile2 : permInherited2;
        expectedAcls = fstat.isFile() ? aclFileSpec2 : aclInheritedSpec2;

        verify(path, expectedPerm, perm, expectedAcls, acls);
        break;
      default:
        throw new RuntimeException("Only 2 permissions by this test: " + permIndex);
    }
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }

  /**
   * Create a new AclEntry with scope, type, name and permission.
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param name
   *          String optional ACL entry name
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name, FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type).setName(name)
        .setPermission(permission).build();
  }

  private void verify(Path path, FsPermission expectedPerm, FsPermission actualPerm,
      List<AclEntry> expectedAcls, List<AclEntry> actualAcls) {
    verifyPermissions(path, expectedPerm, actualPerm);
    verifyAcls(path, expectedAcls, actualAcls);
  }

  private void verifyPermissions(Path path, FsPermission expected, FsPermission actual) {
    LOG.debug("Verify permissions on " + path + " expected=" + expected + " actual=" + actual);

    Assert.assertTrue("User permissions on " + path + " differ: expected=" + expected + " actual=" + actual,
        expected.getUserAction() == actual.getUserAction());
    Assert.assertTrue("Group/Mask permissions on " + path + " differ: expected=" + expected + " actual=" + actual,
        expected.getGroupAction() == actual.getGroupAction());
    Assert.assertTrue("Other permissions on " + path + " differ: expected=" + expected + " actual=" + actual,
        expected.getOtherAction() == actual.getOtherAction());
  }

  private void verifyAcls(Path path, List<AclEntry> expected, List<AclEntry> actual) {
    LOG.debug("Verify ACLs on " + path + " expected=" + expected + " actual=" + actual);

    ArrayList<AclEntry> acls = new ArrayList<AclEntry>(actual);

    for (AclEntry expectedAcl : expected) {
      boolean found = false;
      for (AclEntry acl : acls) {
        if (acl.equals(expectedAcl)) {
          acls.remove(acl);
          found = true;
          break;
        }
      }

      Assert.assertTrue("ACLs on " + path + " differ: " + expectedAcl + " expected=" + expected + " actual=" + actual, found);
    }

    Assert.assertTrue("ACLs on " + path + " are more than expected: expected=" + expected + " actual=" + actual, acls.size() == 0);
  }

  private List<AclEntry> getAcl(String locn) throws Exception {
    return fs.getAclStatus(new Path(locn)).getEntries();
  }
}
