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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.Assert;
import org.junit.Before;

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
    MetastoreConf.setBoolVar(clientConf, ConfVars.METASTORE_CLIENT_USE_PLAIN_AUTH, true);
    MetastoreConf.setBoolVar(clientConf, ConfVars.EXECUTE_SET_UGI, false);
    String tmpDir = System.getProperty("build.dir");
    String credentialsPath = "jceks://file" + tmpDir + "/test-classes/creds/hms_plain_auth_test.jceks";
    clientConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credentialsPath);

    try {
      MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, wrongUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (Exception e) {
      gotException = true;
    }
    // Trying to log in using wrong username and password should fail
    Assert.assertTrue(gotException);

    MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, correctUser);
    return new HiveMetaStoreClient(clientConf);
  }
}
