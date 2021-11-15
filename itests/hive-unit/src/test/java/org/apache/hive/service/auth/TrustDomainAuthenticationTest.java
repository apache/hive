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
package org.apache.hive.service.auth;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract implementation of TrustDomainAuthentication Test.
 *
 */
public abstract class TrustDomainAuthenticationTest {
  private static final Logger LOG = LoggerFactory.getLogger(TrustDomainAuthenticationTest.class);
  private static HiveServer2 hiveserver2;
  private static HiveConf hiveConf;
  private static byte[] hiveConfBackup;
  private static String correctUser = "hive";
  private static String correctPassword = "passwd";
  private static String wrongPassword = "wrong_password";
  private static String wrongUser = "wrong_user";
  static final String HS2_TRANSPORT_MODE_BINARY = "binary";
  static final String HS2_TRANSPORT_MODE_HTTP = "http";
  private static String hs2TransportMode;
  private static boolean properTrustedDomain;

  static void initialize(String transportMode, boolean useProperTrustedDomain) throws Exception {
    Assert.assertNotNull(transportMode);
    Assert.assertTrue(transportMode.equals(HS2_TRANSPORT_MODE_HTTP) ||
            transportMode.equals(HS2_TRANSPORT_MODE_BINARY));
    hs2TransportMode = transportMode;
    properTrustedDomain = useProperTrustedDomain;

    hiveConf = new HiveConf();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    hiveConf.writeXml(baos);
    baos.close();
    hiveConfBackup = baos.toByteArray();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, hs2TransportMode);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "CUSTOM");

    // These tests run locally and hence all connections are from localhost. So, when we want to
    // test whether trusted domain setting works, use "localhost". When we want to test
    // otherwise, use some string other than that. Other authentication tests test empty trusted
    // domain so that's not covered under these tests.
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRUSTED_DOMAIN,
            properTrustedDomain ? "localhost" : "no_such_domain");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS,
            "org.apache.hive.service.auth.TrustDomainAuthenticationTest$SimpleAuthenticationProviderImpl");
    FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
    hiveConf.writeXml(fos);
    fos.close();
    hiveserver2 = new HiveServer2();
    hiveserver2.init(hiveConf);
    hiveserver2.start();
    Thread.sleep(1000);
    LOG.info("hiveServer2 start ......");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(hiveConf != null && hiveConfBackup != null) {
      FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
      fos.write(hiveConfBackup);
      fos.close();
    }
    if (hiveserver2 != null) {
      hiveserver2.stop();
      hiveserver2 = null;
    }
    Thread.sleep(1000);
    LOG.info("hiveServer2 stop ......");
  }

  // TODO: This test doesn't work since getRemoteHost returns IP address instead of a host name
  @Test
  public void testTrustedDomainAuthentication() throws Exception {
    String port = "10000";
    String urlExtra = "";
    if (hs2TransportMode.equals(HS2_TRANSPORT_MODE_HTTP)) {
      port = "10001";
      urlExtra = ";transportMode=http;httpPath=cliservice";
    }

    String url = "jdbc:hive2://localhost:" + port + "/default" + urlExtra;
    Class.forName("org.apache.hive.jdbc.HiveDriver");

    if (properTrustedDomain) {
      testProperTrustedDomainAuthentication(url);
    } else {
      testImproperTrustedDomainAuthentication(url);
    }
  }

  private void testProperTrustedDomainAuthentication(String url) throws SQLException {
    // When the connection is from a trusted domain any connection is authentic irrespective of
    // user and password
    Connection connection = DriverManager.getConnection(url, correctUser, correctPassword);
    connection.close();

    connection = DriverManager.getConnection(url, wrongUser, correctPassword);
    connection.close();

    connection = DriverManager.getConnection(url, wrongUser, wrongPassword);
    connection.close();

    connection = DriverManager.getConnection(url, correctUser, wrongPassword);
    connection.close();
  }

  private void testImproperTrustedDomainAuthentication(String url) throws Exception {
    // When trusted domain doesn't match requests domain, only the connection with correct user
    // and password goes through.
    Connection connection = DriverManager.getConnection(url, correctUser, correctPassword);
    connection.close();

    String partErrorMessage = "Peer indicated failure: Error validating the login";
    if (hs2TransportMode.equals(HS2_TRANSPORT_MODE_HTTP)) {
      partErrorMessage = "HTTP Response code: 401";
    }

    try (Connection conn = DriverManager.getConnection(url, wrongUser, correctPassword)) {
      Assert.fail("Expected Exception");
    } catch (SQLException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(partErrorMessage));
    }

    try (Connection conn = DriverManager.getConnection(url, wrongUser, wrongPassword)) {
      Assert.fail("Expected Exception");
    } catch (SQLException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(partErrorMessage));
    }

    try (Connection conn = DriverManager.getConnection(url, correctUser, wrongPassword)) {
      Assert.fail("Expected Exception");
    } catch (SQLException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(partErrorMessage));
    }
  }

  public static class SimpleAuthenticationProviderImpl implements PasswdAuthenticationProvider {

    private Map<String, String> userMap = new HashMap<String, String>();

    public SimpleAuthenticationProviderImpl() {
      init();
    }

    private void init(){
      userMap.put(correctUser, correctPassword);
    }

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {

      if(!userMap.containsKey(user)){
        throw new AuthenticationException("Invalid user : "+user);
      }
      if(!userMap.get(user).equals(password)){
        throw new AuthenticationException("Invalid passwd : "+password);
      }
    }
  }
}
