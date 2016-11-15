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

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;

/**
 * TestStorageBasedMetastoreAuthorizationProvider. Test case for
 * StorageBasedAuthorizationProvider, by overriding methods defined in
 * TestMetastoreAuthorizationProvider
 *
 * Note that while we do use the hive driver to test, that is mostly for test
 * writing ease, and it has the same effect as using a metastore client directly
 * because we disable hive client-side authorization for this test, and only
 * turn on server-side auth.
 */
public class TestStorageBasedMetastoreAuthorizationProvider extends
    TestMetastoreAuthorizationProvider {

  @Override
  protected String getAuthorizationProvider(){
    return StorageBasedAuthorizationProvider.class.getName();
  }

  @Override
  protected void allowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    setPermissions(location,"-rwxr--r-t");
  }

  @Override
  protected void disallowCreateInDb(String dbName, String userName, String location)
      throws Exception {
    setPermissions(location,"-r--r--r--");
  }

  @Override
  protected void allowCreateInTbl(String tableName, String userName, String location)
      throws Exception{
    setPermissions(location,"-rwxr--r--");
  }


  @Override
  protected void disallowCreateInTbl(String tableName, String userName, String location)
      throws Exception {
    setPermissions(location,"-r--r--r--");
  }

  @Override
  protected void allowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    setPermissions(location,"-rwxr--r--");
  }

  @Override
  protected void disallowDropOnTable(String tblName, String userName, String location)
      throws Exception {
    setPermissions(location,"-r--r--r--");
  }

  @Override
  protected void allowDropOnDb(String dbName, String userName, String location)
      throws Exception {
    setPermissions(location,"-rwxr--r-t");
  }

  protected void setPermissions(String locn, String permissions) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), clientHiveConf);
    fs.setPermission(new Path(locn), FsPermission.valueOf(permissions));
  }

  @Override
  protected void assertNoPrivileges(MetaException me){
    assertNotNull(me);
    assertTrue(me.getMessage().indexOf("AccessControlException") != -1);
  }

  @Override
  protected String getTestDbName(){
    return super.getTestDbName() + "_SBAP";
  }

  @Override
  protected String getTestTableName(){
    return super.getTestTableName() + "_SBAP";
  }

  @Override
  protected void setupMetaStoreReadAuthorization() {
    // enable read authorization in metastore
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname, "true");
  }

}
