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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.DummyHiveMetastoreAuthorizationProvider.AuthCallContext;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test case for verifying that multiple
 * {@link org.apache.hadoop.hive.metastore.AuthorizationPreEventListener}s can
 * be set and they get called.
 */
public class TestMultiAuthorizationPreEventListener {
  private static HiveConf clientHiveConf;
  private static HiveMetaStoreClient msc;
  private static Driver driver;

  @BeforeClass
  public static void setUp() throws Exception {


    int port = MetaStoreUtils.findFreePort();

    System.setProperty(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname,
        AuthorizationPreEventListener.class.getName());

    // Set two dummy classes as authorizatin managers. Two instances should get created.
    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER.varname,
        DummyHiveMetastoreAuthorizationProvider.class.getName() + ","
            + DummyHiveMetastoreAuthorizationProvider.class.getName());

    System.setProperty(HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname,
        HadoopDefaultMetastoreAuthenticator.class.getName());

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());

    clientHiveConf = new HiveConf();

    clientHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    clientHiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    SessionState.start(new CliSessionState(clientHiveConf));
    msc = new HiveMetaStoreClient(clientHiveConf, null);
    driver = new Driver(clientHiveConf);
  }

  @Test
  public void testMultipleAuthorizationListners() throws Exception {
    String dbName = "hive" + this.getClass().getSimpleName().toLowerCase();
    List<AuthCallContext> authCalls = DummyHiveMetastoreAuthorizationProvider.authCalls;
    int listSize = 0;
    assertEquals(listSize, authCalls.size());

    driver.run("create database " + dbName);
    // verify that there are two calls because of two instances of the authorization provider
    listSize = 2;
    assertEquals(listSize, authCalls.size());

    // verify that the actual action also went through
    Database db = msc.getDatabase(dbName);
    listSize += 2; // 1 read database auth calls for each authorization provider
    Database dbFromEvent = (Database)assertAndExtractSingleObjectFromEvent(listSize, authCalls,
        DummyHiveMetastoreAuthorizationProvider.AuthCallContextType.DB);
    validateCreateDb(db,dbFromEvent);
  }

  public Object assertAndExtractSingleObjectFromEvent(int listSize,
      List<AuthCallContext> authCalls,
      DummyHiveMetastoreAuthorizationProvider.AuthCallContextType callType) {
    assertEquals(listSize, authCalls.size());
    assertEquals(1,authCalls.get(listSize-1).authObjects.size());

    assertEquals(callType,authCalls.get(listSize-1).type);
    return (authCalls.get(listSize-1).authObjects.get(0));
  }


  private void validateCreateDb(Database expectedDb, Database actualDb) {
    assertEquals(expectedDb.getName(), actualDb.getName());
    assertEquals(expectedDb.getLocationUri(), actualDb.getLocationUri());
  }


}
