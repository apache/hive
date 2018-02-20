/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.server;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.is;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * TestHS2HttpServerPamConfiguration -- checks configuration for HiveServer2 HTTP Server with Pam authentication
 */
public class TestHS2HttpServerPamConfiguration {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static HiveServer2 hiveServer2 = null;
  private static HiveConf hiveConf = null;
  private static String keyStorePassword = "123456";
  private static String keyFileName = "myKeyStore";
  private static String testDataDir = new File(
      System.getProperty("java.io.tmpdir") + File.separator + TestHS2HttpServerPam.class.getCanonicalName() + "-"
          + System.currentTimeMillis()).getPath().replaceAll("\\\\", "/");
  private static String sslKeyStorePath = testDataDir + File.separator + keyFileName;


  @BeforeClass
  public static void beforeTests() throws Exception {
    createTestDir();
    createDefaultKeyStore();
    String metastorePasswd = "693efe9fa425ad21886d73a0fa3fbc70"; //random md5
    Integer webUIPort =
        MetaStoreTestUtils.findFreePortExcepting(Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM, true);
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, false);
    hiveConf.set(ConfVars.METASTOREPWD.varname, metastorePasswd);
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, webUIPort.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  @Test
  public void testSslIsFalse() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL.varname
        + " has false value. It is recommended to set to true when PAM is used."));
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PAM_SERVICES, "sshd");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL, false);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
  }

  @Test
  public void testPamServicesAreNotConfigured() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(is(ConfVars.HIVE_SERVER2_PAM_SERVICES.varname + " are not configured."));
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PAM_SERVICES, "");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH, sslKeyStorePath);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD, keyStorePassword);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
  }

  @Test
  public void testPamCorrectConfiguration() {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PAM_SERVICES, "sshd");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH, sslKeyStorePath);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD, keyStorePassword);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
  }

  @AfterClass
  public static void afterTests() throws IOException {
    FileUtils.deleteDirectory(new File(testDataDir));
  }

  private static void createTestDir() {
    if (!(new File(testDataDir).mkdirs())) {
      throw new RuntimeException("Could not create " + testDataDir);
    }
  }

  private static void createDefaultKeyStore()
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    char[] password = keyStorePassword.toCharArray();
    ks.load(null, null);

    // Store away the keystore.
    try (FileOutputStream fos = new FileOutputStream(sslKeyStorePath)) {
      ks.store(fos, password);
    }
  }
}
