/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.After;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.collect.Lists;

public class TestStorageBasedMetastoreAuthorizationProviderWithACL
  extends TestStorageBasedMetastoreAuthorizationProvider {

  protected static MiniDFSShim dfs = null;
  protected static Path warehouseDir = null;
  protected static Path extWarehouseDir = null;
  protected UserGroupInformation userUgi = null;
  protected String testUserName = "test_user";
  protected String proxyUserName = null;

  @Override
  protected String getProxyUserName() {
    return proxyUserName;
  }

  @Override
  protected boolean isTestEnabled() {
    // This test with HDFS ACLs will only work if FileSystem.access() is available in the
    // version of hadoop-2 used to build Hive.
    return doesAccessAPIExist();
  }

  private static boolean doesAccessAPIExist() {
    boolean foundMethod = false;
    try {
      Method method = FileSystem.class.getMethod("access", Path.class, FsAction.class);
      foundMethod = true;
    } catch (NoSuchMethodException err) {
    }
    return foundMethod;
  }

  @Override
  protected HiveConf createHiveConf() throws Exception {
    userUgi = UserGroupInformation.createUserForTesting(testUserName, new String[] {});

    // Hadoop FS ACLs do not work with LocalFileSystem, so set up MiniDFS.
    HiveConf conf = super.createHiveConf();
    proxyUserName = Utils.getUGI().getShortUserName();
    conf.set("dfs.namenode.acls.enabled", "true");
    conf.set("hadoop.proxyuser." + proxyUserName + ".groups", "*");
    conf.set("hadoop.proxyuser." + proxyUserName + ".hosts", "*");
    dfs = ShimLoader.getHadoopShims().getMiniDfs(conf, 4, true, null);
    FileSystem fs = dfs.getFileSystem();

    warehouseDir = new Path(new Path(fs.getUri()), "/warehouse");
    fs.mkdirs(warehouseDir);
    conf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE, warehouseDir.toString());
    extWarehouseDir = new Path(new Path(fs.getUri()), "/external");
    fs.mkdirs(extWarehouseDir);
    conf.setVar(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL, extWarehouseDir.toString());

    // Set up scratch directory
    Path scratchDir = new Path(new Path(fs.getUri()), "/scratchdir");
    conf.setVar(HiveConf.ConfVars.SCRATCH_DIR, scratchDir.toString());

    return conf;
  }

  protected String setupUser() {
    // Using MiniDFS, the permissions don't work properly because
    // the current user gets treated as a superuser.
    // For this test, specify a different (non-super) user. 
    InjectableDummyAuthenticator.injectUserName(userUgi.getShortUserName());
    InjectableDummyAuthenticator.injectGroupNames(Arrays.asList(userUgi.getGroupNames()));
    InjectableDummyAuthenticator.injectMode(true);
    return userUgi.getShortUserName();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();

    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }

  protected void allowWriteAccessViaAcl(String userName, String location)
      throws Exception {
    // Set the FS perms to read-only access, and create ACL entries allowing write access.
    List<AclEntry> aclSpec = Lists.newArrayList(
        aclEntry(ACCESS, USER, FsAction.READ_EXECUTE),
        aclEntry(ACCESS, GROUP, FsAction.READ_EXECUTE),
        aclEntry(ACCESS, OTHER, FsAction.READ_EXECUTE),
        aclEntry(ACCESS, USER, userName, FsAction.ALL)
        );
    FileSystem fs = FileSystem.get(new URI(location), clientHiveConf);
    fs.setAcl(new Path(location), aclSpec);
  }

  protected void disallowWriteAccessViaAcl(String userName, String location)
      throws Exception {
    FileSystem fs = FileSystem.get(new URI(location), clientHiveConf);
    fs.removeAcl(new Path(location));
    setPermissions(location,"-r-xr-xr-x");
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   * Borrowed from TestExtendedAcls
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
   * Borrowed from TestExtendedAcls
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

  @Override
  protected boolean mayTestLocation() {
    return false;
  }

  @Override
  protected void allowCreateDatabase(String userName)
      throws Exception {
    allowWriteAccessViaAcl(userName, warehouseDir.toString());
    allowWriteAccessViaAcl(userName, extWarehouseDir.toString());
  }

  @Override
  protected void disallowCreateDatabase(String userName)
      throws Exception {
    disallowWriteAccessViaAcl(userName, warehouseDir.toString());
    disallowWriteAccessViaAcl(userName, extWarehouseDir.toString());
  }

  @Override
  protected void allowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    allowWriteAccessViaAcl(userName, location);
  }

  @Override
  protected void disallowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    disallowWriteAccessViaAcl(userName, location);
  }

  @Override
  protected void allowCreateInTbl(String tableName, String userName, String location)
      throws Exception{
    allowWriteAccessViaAcl(userName, location);
  }


  @Override
  protected void disallowCreateInTbl(String tableName, String userName, String location)
      throws Exception {
    disallowWriteAccessViaAcl(userName, location);
  }

  @Override
  protected void allowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    allowWriteAccessViaAcl(userName, location);
  }

  @Override
  protected void disallowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    disallowWriteAccessViaAcl(userName, location);
  }

  @Override
  protected void allowDropOnDb(String dbName, String userName, String location)
      throws Exception {
    allowWriteAccessViaAcl(userName, location);
  }
}
