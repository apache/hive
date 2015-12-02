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
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestExtendedAcls extends FolderPermissionBase {

  @BeforeClass
  public static void setup() throws Exception {
    conf = new HiveConf(TestExtendedAcls.class);
    //setup the mini DFS with acl's enabled.
    conf.set("dfs.namenode.acls.enabled", "true");
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    baseSetup();
  }

  private final ImmutableList<AclEntry> aclSpec1 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.ALL),
      aclEntry(ACCESS, USER, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar", FsAction.READ_WRITE),
      aclEntry(ACCESS, GROUP, "foo", FsAction.READ_EXECUTE));

  private final ImmutableList<AclEntry> aclSpec2 = ImmutableList.of(
      aclEntry(ACCESS, USER, FsAction.ALL),
      aclEntry(ACCESS, GROUP, FsAction.ALL),
      aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
      aclEntry(ACCESS, USER, "bar2", FsAction.READ_WRITE),
      aclEntry(ACCESS, USER, "foo2", FsAction.READ_EXECUTE),
      aclEntry(ACCESS, GROUP, "bar2", FsAction.READ),
      aclEntry(ACCESS, GROUP, "foo2", FsAction.READ_EXECUTE));

  @Override
  public void setPermission(String locn, int permIndex) throws Exception {
    switch (permIndex) {
      case 0:
        setAcl(locn, aclSpec1);
        break;
      case 1:
        setAcl(locn, aclSpec2);
        break;
      default:
        throw new RuntimeException("Only 2 permissions by this test");
    }
  }

  @Override
  public void verifyPermission(String locn, int permIndex) throws Exception {
    switch (permIndex) {
      case 0:
        FsPermission perm = fs.getFileStatus(new Path(locn)).getPermission();
        Assert.assertEquals("Location: " + locn, "rwxrwxrwx", String.valueOf(perm));

        List<AclEntry> actual = getAcl(locn);
        verifyAcls(aclSpec1, actual);
        break;
      case 1:
        perm = fs.getFileStatus(new Path(locn)).getPermission();
        Assert.assertEquals("Location: " + locn, "rwxrwxr-x", String.valueOf(perm));

        List<AclEntry> acls = getAcl(locn);
        verifyAcls(aclSpec2, acls);
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

  private void verifyAcls(List<AclEntry> expectedList, List<AclEntry> actualList) {
    for (AclEntry expected : expectedList) {
      if (expected.getName() != null) {
        //the non-named acl's are coming as regular permission, and not as aclEntries.
        boolean found = false;
        for (AclEntry actual : actualList) {
          if (actual.equals(expected)) {
            found = true;
          }
        }
        if (!found) {
          Assert.fail("Following Acl does not have a match: " + expected);
        }
      }
    }
  }

  private void setAcl(String locn, List<AclEntry> aclSpec) throws Exception {
    fs.setAcl(new Path(locn), aclSpec);
  }

  private List<AclEntry> getAcl(String locn) throws Exception {
    return fs.getAclStatus(new Path(locn)).getEntries();
  }
}
