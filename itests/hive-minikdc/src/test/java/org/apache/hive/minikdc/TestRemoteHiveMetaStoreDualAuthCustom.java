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

package org.apache.hive.minikdc;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStorePasswdAuthenticationProvider;
import org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStore;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import javax.security.sasl.AuthenticationException;
import java.util.HashMap;
import java.util.Map;

@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveMetaStoreDualAuthCustom extends RemoteHiveMetaStoreDualAuthTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    boolean gotException = false;
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(clientConf, ConfVars.USE_THRIFT_SASL, false);
    MetastoreConf.setBoolVar(clientConf, ConfVars.USE_THRIFT_PASSWORD_AUTH, true);
    MetastoreConf.setBoolVar(clientConf, ConfVars.EXECUTE_SET_UGI, false);

    try {
      MetastoreConf.setVar(clientConf, ConfVars.THRIFT_AUTH_USERNAME, wrongUser);
      MetastoreConf.setVar(clientConf, ConfVars.THRIFT_AUTH_PASSWORD, wrongPassword);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (Exception e) {
      gotException = true;
    }
    // Trying to log in using wrong username and password should fail
    Assert.assertTrue(gotException);

    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_AUTH_USERNAME, correctUser);
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_AUTH_PASSWORD, correctPassword);
    return new HiveMetaStoreClient(clientConf);
  }
}
