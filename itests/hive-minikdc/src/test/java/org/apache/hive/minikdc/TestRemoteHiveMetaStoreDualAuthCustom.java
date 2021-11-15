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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

public class TestRemoteHiveMetaStoreDualAuthCustom extends RemoteHiveMetaStoreDualAuthTest {
  private static String testDataDir = new File(
          System.getProperty("java.io.tmpdir") + File.separator +
                  TestRemoteHiveMetaStoreDualAuthCustom.class.getCanonicalName() + "-"
                  + System.currentTimeMillis()).getPath().replaceAll("\\\\", "/");

  @BeforeClass
  public static void beforeTests() throws Exception {
    createTestDir();
  }

  @AfterClass
  public static void afterTests() throws IOException {
    FileUtils.deleteDirectory(new File(testDataDir));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  private static void createTestDir() {
    if (!(new File(testDataDir).mkdirs())) {
      throw new RuntimeException("Could not create " + testDataDir);
    }
  }

  // The function creates a JCEKS file with given userName as key and passWord as alias and
  // returns its provider path.
  private String createCredFile(String userName, String passWord) throws Exception {
    String fileName = "hms_auth_" + userName + "_" + passWord + "." + JavaKeyStoreProvider.SCHEME_NAME;
    String credUrl =
            JavaKeyStoreProvider.SCHEME_NAME + "://file" + testDataDir + File.separator + fileName;
    Configuration credConf = new Configuration();
    credConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
    CredentialProvider provider = CredentialProviderFactory.getProviders(credConf).get(0);
    provider.createCredentialEntry(userName, passWord.toCharArray());
    provider.flush();
    return credUrl;
  }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
    MetastoreConf.setVar(clientConf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(clientConf, ConfVars.USE_THRIFT_SASL, false);
    MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_AUTH_MODE, "PLAIN");
    MetastoreConf.setBoolVar(clientConf, ConfVars.EXECUTE_SET_UGI, false);

    // Trying to log in using correct username but wrong password should fail
    String credsCUWP = createCredFile(correctUser, wrongPassword);
    clientConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsCUWP);
    String exceptionMessage = null;
    try {
      MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, correctUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("Error validating the login"));

    // Trying to log in with a user whose credentials do not exist in the given file should fail.
    exceptionMessage = null;
    try {
      MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, wrongUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("No password found for user"));

    // Trying to login with a use who does not exist but whose password is found in credential
    // file should fail. It doesn't matter what the password is since the user doesn't exist
    exceptionMessage = null;
    String credsWUWP = createCredFile(wrongUser, wrongPassword);
    clientConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsWUWP);
    try {
      MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, wrongUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(clientConf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("Error validating the login"));

    // correct_user and correct_password creds file
    String credsCUCP = createCredFile(correctUser, correctPassword);
    clientConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsCUCP);
    MetastoreConf.setVar(clientConf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, correctUser);
    return new HiveMetaStoreClient(clientConf);
  }
}
