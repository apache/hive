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

package org.apache.hadoop.hive.metastore;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
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
import org.junit.experimental.categories.Category;

import javax.security.sasl.AuthenticationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveMetaStoreCustomAuth extends TestRemoteHiveMetaStore {
  private static String correctUser = "correct_user";
  private static String correctPassword = "correct_passwd";
  private static String wrongPassword = "wrong_password";
  private static String wrongUser = "wrong_user";
  private static String testDataDir = new File(
          System.getProperty("java.io.tmpdir") + File.separator +
                  TestRemoteHiveMetaStoreCustomAuth.class.getCanonicalName() + "-"
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
    initConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION, "CUSTOM");
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CUSTOM_AUTHENTICATION_CLASS,
            "org.apache.hadoop.hive.metastore.TestRemoteHiveMetaStoreCustomAuth$SimpleAuthenticationProviderImpl");
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
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
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE, "PLAIN");

    // Trying to log in using correct username but wrong password should fail
    String credsCUWP = createCredFile(correctUser, wrongPassword);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsCUWP);
    String exceptionMessage = null;
    try {
      MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, correctUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("Error validating the login"));

    // Trying to log in with a user whose credentials do not exist in the given file should fail.
    exceptionMessage = null;
    try {
      MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, wrongUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("No password found for user"));

    // Trying to login with a use who does not exist but whose password is found in credential
    // file should fail. It doesn't matter what the password is since the user doesn't exist
    exceptionMessage = null;
    String credsWUWP = createCredFile(wrongUser, wrongPassword);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsWUWP);
    try {
      MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, wrongUser);
      HiveMetaStoreClient tmpClient = new HiveMetaStoreClient(conf);
    } catch (MetaException e) {
      exceptionMessage = e.getMessage();
    }
    Assert.assertNotNull(exceptionMessage);
    Assert.assertTrue(exceptionMessage.contains("Error validating the login"));

    // correct_user and correct_password creds file
    String credsCUCP = createCredFile(correctUser, correctPassword);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credsCUCP);
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_PLAIN_USERNAME, correctUser);
    return new HiveMetaStoreClient(conf);
  }

  public static class SimpleAuthenticationProviderImpl implements MetaStorePasswdAuthenticationProvider {

    private Map<String, String> userMap = new HashMap<>();

    public SimpleAuthenticationProviderImpl() {
      init();
    }

    private void init(){
      userMap.put(correctUser, correctPassword);
    }

    @Override
    public void authenticate(String user, String password) throws AuthenticationException {

      if(!userMap.containsKey(user)) {
        throw new AuthenticationException("Invalid user : " + user);
      }
      if(!userMap.get(user).equals(password)){
        throw new AuthenticationException("Invalid passwd : " + password);
      }
    }
  }
}
