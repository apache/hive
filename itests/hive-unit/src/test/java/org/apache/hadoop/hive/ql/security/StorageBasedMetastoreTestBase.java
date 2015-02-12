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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.WindowsPathUtil;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class for some storage based authorization test classes
 */
public class StorageBasedMetastoreTestBase {
  protected HiveConf clientHiveConf;
  protected HiveMetaStoreClient msc;
  protected Driver driver;
  protected UserGroupInformation ugi;
  private static int objNum = 0;

  protected String getAuthorizationProvider(){
    return StorageBasedAuthorizationProvider.class.getName();
  }

  protected HiveConf createHiveConf() throws Exception {
    HiveConf conf = new HiveConf(this.getClass());
    if (Shell.WINDOWS) {
      WindowsPathUtil.convertPathsFromWindowsToHdfs(conf);
    }
    return conf;
  }

  @Before
  public void setUp() throws Exception {

    int port = MetaStoreUtils.findFreePort();

    // Turn on metastore-side authorization
    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        getAuthorizationProvider());
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        InjectableDummyAuthenticator.class.getName());

    clientHiveConf = createHiveConf();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), clientHiveConf);

    // Turn off client-side authorization
    clientHiveConf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED,false);

    clientHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    clientHiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    clientHiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    clientHiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");

    ugi = Utils.getUGI();

    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf, null);
    driver = new Driver(clientHiveConf);

    setupFakeUser();
    InjectableDummyAuthenticator.injectMode(false);
  }

  protected void setupFakeUser() {
    String fakeUser = "mal";
    List<String> fakeGroupNames = new ArrayList<String>();
    fakeGroupNames.add("groupygroup");

    InjectableDummyAuthenticator.injectUserName(fakeUser);
    InjectableDummyAuthenticator.injectGroupNames(fakeGroupNames);
  }

  protected String setupUser() {
    return ugi.getUserName();
  }

  protected String getTestTableName() {
    return this.getClass().getSimpleName() + "tab" + ++objNum;
  }

  protected String getTestDbName() {
    return this.getClass().getSimpleName() + "db" + ++objNum;
  }

  @After
  public void tearDown() throws Exception {
    InjectableDummyAuthenticator.injectMode(false);
  }

  protected void setPermissions(String locn, String permissions) throws Exception {
    FileSystem fs = FileSystem.get(new URI(locn), clientHiveConf);
    fs.setPermission(new Path(locn), FsPermission.valueOf(permissions));
  }

  protected void validateCreateDb(Database expectedDb, String dbName) {
    Assert.assertEquals(expectedDb.getName().toLowerCase(), dbName.toLowerCase());
  }


}
