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

package org.apache.hive.service.auth.saml;

import static org.apache.hive.jdbc.Utils.JdbcConnectionParams.AUTH_BROWSER_RESPONSE_PORT;
import static org.apache.hive.jdbc.Utils.JdbcConnectionParams.AUTH_BROWSER_RESPONSE_TIMEOUT_SECS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.ql.metadata.TestHive;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.saml.HiveJdbcBrowserClient;
import org.apache.hive.jdbc.saml.IJdbcBrowserClient;
import org.apache.hive.jdbc.saml.IJdbcBrowserClient.HiveJdbcBrowserException;
import org.apache.hive.jdbc.saml.IJdbcBrowserClientFactory;
import org.apache.hive.jdbc.saml.SimpleSAMLPhpTestBrowserClient;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End to end tests for the SAML based SSO authentication. The test instantiates a SAML
 * 2.0 compliant IDP provider (SimpleSAMLPHP) in a docker container. The container is
 * configured such that it's IDP service is run on a random port which is available. See
 * {@link #setupIDP(boolean, String)} for details.
 * <p>
 * The HS2 is configured with the IDP's metadata file which is derived from a template.
 * The metadata file contains the IDP's public certificate. The template should be
 * post-processed to replace the port number where the service is running.
 * <p>
 * The test uses TestContainers library for managing the life-cycle of the IDP container.
 * It uses HTMLUnit to simulate the browser interaction (provide user/password) of the
 * end user.
 */
public class TestHttpSamlAuthentication {

  //user credentials. These much match with the authsources.php of the simpleSAMLPHP
  //IDP and the auth mode must be USER_PASS_MODE
  private static final String USER1 = "user1";
  private static final String USER1_PASSWORD = "user1pass";
  private static final String USER2 = "user2";
  private static final String USER2_PASSWORD = "user2pass";
  private static final String USER3 = "user3";
  private static final String USER3_PASSWORD = "user3pass";
  private static final String IDP_GROUP_ATTRIBUTE_NAME = "eduPersonAffiliation";
  private static MiniHS2 miniHS2;
  // needs user/password to login to the IDP.
  private static final String USER_PASS_MODE = "example-userpass";

  public static GenericContainer idpContainer;
  private static final File tmpDir = Files.createTempDir();
  private static final File idpMetadataFile = new File(tmpDir, "idp-metadata.xml");

  @BeforeClass
  public static void setupHS2() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
    conf.setBoolVar(ConfVars.HIVESTATSCOLAUTOGATHER, false);
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "SAML");
    conf.setVar(ConfVars.HIVE_SERVER2_SAML_IDP_METADATA,
        idpMetadataFile.getAbsolutePath());
    conf.setVar(ConfVars.HIVE_SERVER2_SAML_SP_ID, "test-hive-SAML-sp");
    conf.setVar(ConfVars.HIVE_SERVER2_SAML_KEYSTORE_PATH,
        new File(tmpDir, "saml_keystore.jks").getAbsolutePath());
    conf.setVar(ConfVars.HIVE_SERVER2_SAML_KEYSTORE_PASSWORD, "test-password");
    conf.setVar(ConfVars.HIVE_SERVER2_SAML_PRIVATE_KEY_PASSWORD, "secret");
    miniHS2 = new MiniHS2.Builder().withConf(conf).withHTTPTransport().build();
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
      miniHS2.cleanup();
      miniHS2 = null;
      MiniHS2.cleanupLocalDir();
    }
    if (tmpDir.exists()) {
      tmpDir.delete();
    }
    if (idpContainer != null && idpContainer.isRunning()) {
      idpContainer.stop();
    }
  }

  @After
  public void cleanUpIdpEnv() {
    if (idpContainer != null) {
      idpContainer.stop();
      idpContainer = null;
    }
    if (miniHS2 != null) {
      miniHS2.stop();
    }
  }

  private void setupIDP(boolean useSignedAssertions, String authMode) throws Exception {
    setupIDP(useSignedAssertions, authMode, null, null);
  }

  private void setupIDP(boolean useSignedAssertions, String authMode, List<String> groups,
      String tokenExpirySecs) throws Exception {
    Map<String, String> configOverlay = new HashMap<>();
    configOverlay.put(ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL.varname,
        "http://localhost:" + miniHS2.getHttpPort()
            + "/sso/saml?client_name=HiveSaml2Client");
    if (groups != null) {
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_GROUP_ATTRIBUTE_NAME.varname,
          IDP_GROUP_ATTRIBUTE_NAME);
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_GROUP_FILTER.varname,
          Joiner.on(',').join(groups));
    } else {
      // reset the configs since the previous test may have set them.
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_GROUP_ATTRIBUTE_NAME.varname, "");
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_GROUP_FILTER.varname, "");
    }
    if (tokenExpirySecs != null) {
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_CALLBACK_TOKEN_TTL.varname,
          tokenExpirySecs);
    } else {
      configOverlay.put(ConfVars.HIVE_SERVER2_SAML_CALLBACK_TOKEN_TTL.varname,
          ConfVars.HIVE_SERVER2_SAML_CALLBACK_TOKEN_TTL.defaultStrVal);
    }
    miniHS2.start(configOverlay);
    Map<String, String> idpEnv = getIdpEnv(useSignedAssertions, authMode);
    idpContainer = new GenericContainer<>(
        DockerImageName.parse("vihangk1/docker-test-saml-idp"))
        .withExposedPorts(8080, 8443)
        .withEnv(idpEnv);
    idpContainer.start();
    Integer ssoPort = idpContainer.getMappedPort(8080);
    writeIdpMetadataFile(ssoPort, idpMetadataFile);
  }

  private static void writeIdpMetadataFile(Integer ssoPort, File targetFile)
      throws IOException {
    String metadata = Resources.toString(
        Resources.getResource("simple-saml-idp-metadata-template.xml"),
        Charsets.UTF_8);
    metadata = metadata.replace("REPLACE_ME", ssoPort.toString());
    Files.write(metadata, targetFile, StandardCharsets.UTF_8);
  }

  private static Map<String, String> getIdpEnv(boolean useSignedAssertions,
      String authMode) throws Exception {
    Map<String, String> idpEnv = new HashMap<>();
    idpEnv
        .put("SIMPLESAMLPHP_SP_ASSERTION_CONSUMER_SERVICE", HiveSamlUtils.getCallBackUri(
            miniHS2.getServerConf()).toString());
    idpEnv.put("SIMPLESAMLPHP_SP_ENTITY_ID",
        miniHS2.getHiveConf().get(ConfVars.HIVE_SERVER2_SAML_SP_ID.varname));
    idpEnv.put("SIMPLESAMLPHP_SP_NAME_ID_FORMAT",
        "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress");
    idpEnv.put("SIMPLESAMLPHP_SP_NAME_ID_ATTRIBUTE", "email");
    idpEnv.put("SIMPLESAMLPHP_IDP_AUTH", authMode);
    // by default idp signs the assertions
    if (!useSignedAssertions) {
      idpEnv.put("SIMPLESAMLPHP_SP_SIGN_ASSERTION", "false");
    }
    return idpEnv;
  }

  private static String getSamlJdbcConnectionUrl() throws Exception {
    return miniHS2.getHttpJdbcURL() + "auth=browser;";
  }

  private static String getSamlJdbcConnectionUrl(int timeoutInSecs) throws Exception {
    return getSamlJdbcConnectionUrl() + AUTH_BROWSER_RESPONSE_TIMEOUT_SECS
        + "=" + timeoutInSecs;
  }

  private static String getSamlJdbcConnectionUrl(int timeoutInSecs, int responsePort)
      throws Exception {
    return getSamlJdbcConnectionUrl(timeoutInSecs) + ";" + AUTH_BROWSER_RESPONSE_PORT
        + "=" + responsePort;
  }

  private static String getSamlJdbcConnectionUrlWithRetry(int numRetries)
      throws Exception {
    return getSamlJdbcConnectionUrl() + ";" + JdbcConnectionParams.RETRIES + "="
        + numRetries;
  }

  private static class TestHiveJdbcBrowserClientFactory implements
      IJdbcBrowserClientFactory {

    private final String user;
    private final String password;
    // tokenDelayMs introduces a delay in browser client before it sends it to the server
    // as the bearer token. Used for testing the expiry of the token.
    private final long tokenDelayMs;

    TestHiveJdbcBrowserClientFactory(String user, String password, long tokenDelayMs) {
      this.user = user;
      this.password = password;
      this.tokenDelayMs = tokenDelayMs;
    }

    @Override
    public IJdbcBrowserClient create(JdbcConnectionParams connectionParams)
        throws HiveJdbcBrowserException {
      return new SimpleSAMLPhpTestBrowserClient(
          connectionParams, user, password, tokenDelayMs);
    }
  }

  /**
   * Test HiveConnection which injects a HTMLUnit based browser client.
   */
  private static class TestHiveConnection extends HiveConnection {
    public TestHiveConnection(String uri, Properties info, String testUser,
        String testPass, long tokenDelayMs) throws SQLException {
      super(uri, info, new TestHiveJdbcBrowserClientFactory(testUser, testPass, tokenDelayMs));
    }

    public TestHiveConnection(String uri, Properties info, String testUser,
        String testPass) throws SQLException {
      super(uri, info, new TestHiveJdbcBrowserClientFactory(testUser, testPass, 0L));
    }

    @Override
    protected void validateSslForBrowserMode() {
      // the tests using non-ssl connection; we skip the validation.
    }
  }

  private static class TestHiveConnectionWithInjectedFailure extends TestHiveConnection {
    private static boolean failureToggle = true;
    public TestHiveConnectionWithInjectedFailure(String uri, Properties info,
        String testUser,
        String testPass) throws SQLException {
      super(uri, info, testUser, testPass);
    }

    @Override
    protected void injectBrowserSSOError() throws Exception {
      if (failureToggle) {
        failureToggle = false;
        throw new Exception("Injected failure");
      }
    }
  }

  /**
   * Util class to issue multiple connection requests concurrently.
   */
  private static class TestHiveConnectionCallable implements Callable<Void> {

    private final String user, password;
    private final int iterations;

    TestHiveConnectionCallable(String user, String password, int iterations) {
      this.user = user;
      this.password = password;
      this.iterations = iterations;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < iterations; i++) {
        try (TestHiveConnection connection = new TestHiveConnection(
            getSamlJdbcConnectionUrl(), new Properties(), user, password)) {
          assertLoggedInUser(connection, user);
        }
      }
      return null;
    }
  }

  @Test(expected = SQLException.class)
  public void testFailureForUnsignedResponse() throws Exception {
    setupIDP(false, USER_PASS_MODE);
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER1, USER1_PASSWORD)) {
      fail("Expected a failure since SAML response is not signed");
    }
  }

  /**
   * Tests basic SSO connection using a user/password.
   */
  @Test
  public void testBasicConnection() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER1, USER1_PASSWORD)) {
      assertLoggedInUser(connection, USER1);
    }
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER2, USER2_PASSWORD)) {
      assertLoggedInUser(connection, USER2);
    }
  }

  /**
   * Tests multiple concurrent SSO connections. Make sure that the logged in user is same
   * as the one which is provided in the credentials.
   */
  @Test
  public void testConcurrentConnections() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    List<Future<Void>> futures = new ArrayList<>();
    futures.add(executorService
        .submit(new TestHiveConnectionCallable(USER1, USER1_PASSWORD, 11)));
    futures.add(executorService
        .submit(new TestHiveConnectionCallable(USER2, USER2_PASSWORD, 17)));
    futures.add(executorService
        .submit(new TestHiveConnectionCallable(USER3, USER3_PASSWORD, 13)));
    for (Future<Void> f : futures) {
      f.get();
    }
  }

  /**
   * Test makes sure that the if the user doesn't provide the right credentials the
   * connection timesout after the given timeout value.
   */
  @Test(expected = SQLException.class)
  public void testConnectionTimeout() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(2), new Properties(), USER1, USER2_PASSWORD)) {
      fail(USER1 + " was logged in even with incorrect password");
    } catch (SQLException e) {
//      assertTrue("Unexpected error message", e.getMessage().contains(
//          HiveJdbcBrowserClient.TIMEOUT_ERROR_MSG));
      throw e;
    }
  }

  /**
   * Test makes sure that if a saml response port is provided on the URL, the browser
   * client opens the given port. We retry one more time in case of failure since the
   * the finding of free port has race conditions (port used immediately later by someone
   * else).
   */
  @Test
  public void testSamlResponsePort() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    for (int i=0; i<2; i++) {
      int samlResponsePort = MetaStoreTestUtils.findFreePort();
      try (TestHiveConnection connection = new TestHiveConnection(
          getSamlJdbcConnectionUrl(30, samlResponsePort), new Properties(), USER1,
          USER1_PASSWORD)) {
        assertLoggedInUser(connection, USER1);
        assertEquals(samlResponsePort,
            connection.getBrowserClient().getPort().intValue());
        break;
      }
    }
  }

  /**
   * Makes sure that the port is released once the SSO flow completes. The test retries
   * twice to make sure that there is no flakiness caused by the race between finding
   * a free port and immediately some other process acquiring that port later before
   * Browser client could be started on it.
   */
  @Test
  public void testPortReleaseAfterSSO() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    for (int i=0; i<2; i++) {
      int samlResponsePort = MetaStoreTestUtils.findFreePort();
      try (TestHiveConnection connection = new TestHiveConnection(
          getSamlJdbcConnectionUrl(30, samlResponsePort), new Properties(), USER1,
          USER1_PASSWORD)) {
        assertLoggedInUser(connection, USER1);
        assertEquals(samlResponsePort,
            connection.getBrowserClient().getPort().intValue());
        assertTrue("Port must be released after SSO flow",
            isPortAvailable(samlResponsePort));
        break;
      }
    }
  }

  /**
   * Here we start HS2 without SAML mode and attempt to use browser SSO against it.
   * Test makes sure that connection fails and port is released.
   */
  @Test
  public void testPortReleaseOnInvalidConfig() throws Exception {
    // no IDP setup here; start HS2 in non-SAML mode
    miniHS2.start(new HashMap<>());
    int samlResponsePort = MetaStoreTestUtils.findFreePort();
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(30, samlResponsePort), new Properties(), USER1,
        USER1_PASSWORD)) {
      fail("Connection should not have succeeded since HS2 is not configured correctly");
    } catch (Exception e) {
      assertTrue("Port must be released after SSO flow",
          isPortAvailable(samlResponsePort));
    }
  }

  /**
   * Make sure that the given port is free by binding to it and then releasing it.
   * @param port
   */
  private boolean isPortAvailable(int port) {
    try (ServerSocket socket = new ServerSocket(port, 0,
        InetAddress.getByName(HiveSamlUtils.LOOP_BACK_INTERFACE))) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Test exercises group name filtering for users. Only users who belong for a given
   * groups should be able to log in. user1 belongs to group1, user2 to group2 and user3
   * to group1 and group2.
   */
  @Test
  public void testGroupNameFiltering() throws Exception {
    setupIDP(true, USER_PASS_MODE, Arrays.asList("group1"), null);
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER1, USER1_PASSWORD)) {
      assertLoggedInUser(connection, USER1);
    }
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER3, USER3_PASSWORD)) {
      assertLoggedInUser(connection, USER3);
    }
    //user2 does not belong to group1 and hence should not be allowed.
    Exception e1 = null;
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER2, USER2_PASSWORD)) {
      fail(USER2 +" does not belong to group1 but could still log in.");
    } catch (SQLException e) {
      e1 = e;
    }
    assertNotNull("Exception was expected but was not received", e1);
  }

  /**
   * Tests group name filtering with multiple group names are configured on the HS2 side.
   */
  @Test
  public void testGroupNameFiltering2() throws Exception {
    setupIDP(true, USER_PASS_MODE, Arrays.asList("group1", "group2"), null);
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER1, USER1_PASSWORD)) {
      assertLoggedInUser(connection, USER1);
    }

    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER2, USER2_PASSWORD)) {
      assertLoggedInUser(connection, USER2);
    }
    try (TestHiveConnection connection = new TestHiveConnection(
        getSamlJdbcConnectionUrl(), new Properties(), USER3, USER3_PASSWORD)) {
      assertLoggedInUser(connection, USER3);
    }
  }


  /**
   * Test injects failure in the first connection attempt and then makes sure that the
   * 2nd retry works as expected.
   */
  @Test
  public void testConnectionRetries() throws Exception {
    setupIDP(true, USER_PASS_MODE);
    try (HiveConnection connection = new TestHiveConnectionWithInjectedFailure(
        getSamlJdbcConnectionUrlWithRetry(2), new Properties(), USER1, USER1_PASSWORD)) {
      assertLoggedInUser(connection, USER1);
    }
  }

  /**
   * Test makes sure that the token received in the server response is not valid
   * after it is expired.
   */
  @Test(expected = SQLException.class)
  public void testTokenTimeout() throws Exception {
    // we set up the HS2 so that the token will expire after 5 seconds
    setupIDP(true, USER_PASS_MODE, null, "3s");
    // we add a token delay of 7 seconds
    try (HiveConnection connection = new TestHiveConnection(getSamlJdbcConnectionUrl(),
        new Properties(), USER1, USER1_PASSWORD, 7000)) {
      fail(USER1 + " logged in even after token expiry");
    }
  }

  /**
   * Test make sure that a token which is issued for a different connection cannot be
   * reused.
   */
  @Test(expected = SQLException.class)
  public void testTokenReuse() throws Exception {
    setupIDP(true, USER_PASS_MODE, null, null);
    String token = null;
    try (HiveConnection connection = new TestHiveConnection(getSamlJdbcConnectionUrl(),
        new Properties(), USER1, USER1_PASSWORD)) {
      token = connection.getBrowserClient().getServerResponse().getToken();
    }
    assertNotNull(token);
    //inject the token using http.header url param
    String bearerToken = "Bearer%20" + token;
    String jdbcUrl =
        getSamlJdbcConnectionUrl(10) + ";http.header.Authorization=" + bearerToken;
    try (HiveConnection connection = new HiveConnection(jdbcUrl, new Properties())) {
      fail("User should not be able to login just using the token");
    }
  }

  private static void assertLoggedInUser(HiveConnection connection, String expectedUser)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select logged_in_user()");
    assertTrue(resultSet.next());
    String loggedInUser = resultSet.getString(1);
    assertEquals(expectedUser, loggedInUser);
  }
}
