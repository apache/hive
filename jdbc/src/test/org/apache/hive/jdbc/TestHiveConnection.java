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

package org.apache.hive.jdbc;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHiveConnection {

  private static final String EXISTING_TOKEN = "ExistingToken";
  public static final String EXPECTED_TOKEN_STRING_FORM = "AAAAAA";
  private static HiveConnection.DelegationTokenFetcher fetcher;

  @BeforeClass
  public static void init() {
    fetcher = new HiveConnection.DelegationTokenFetcher();
  }

  @Test
  public void testIfNPEThrownWhileGettingDelegationToken() throws IOException {
    try {
      String tokenStr = fetcher.getTokenFromCredential(new Credentials(), "hive");
      Assert.assertEquals("Token with id: hive shall not be found.", null, tokenStr);
    } catch (NullPointerException e) {
      Assert.fail("This NPE is not handled in the code elsewhere so user is not notified about it!");
      e.printStackTrace();
    }
  }

  @Test
  public void testIfGettingDelegationTokenFromCredentialWorks() throws IOException {
    Credentials creds = new Credentials();
    creds.addToken(new Text(EXISTING_TOKEN), new Token<>());

    String tokenStr = fetcher.getTokenFromCredential(creds, EXISTING_TOKEN);
    Assert.assertEquals("Token string form is not as expected.", EXPECTED_TOKEN_STRING_FORM, tokenStr);
  }

  @Test
  public void testHiveConnectionParameters() throws SQLException, ZooKeeperHiveClientException {
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;transportMode=http;httpPath=cliservice;socketTimeout=60;requestTrack=true;");

    Assert.assertEquals("hello.host", params.getHost());
    Assert.assertEquals("default", params.getDbName());
    Assert.assertEquals(10002, params.getPort());

    Assert.assertEquals("http", params.getSessionVars().get(JdbcConnectionParams.TRANSPORT_MODE));
    Assert.assertEquals("cliservice", params.getSessionVars().get(JdbcConnectionParams.HTTP_PATH));
    Assert.assertEquals("60", params.getSessionVars().get(JdbcConnectionParams.SOCKET_TIMEOUT));
    Assert.assertEquals("true", params.getSessionVars().get(JdbcConnectionParams.JDBC_PARAM_REQUEST_TRACK));

    JdbcConnectionParams nonPortParams = Utils.parseURL("jdbc:hive2://hello.host/default");
    Assert.assertEquals(10000, nonPortParams.getPort());

    JdbcConnectionParams nonPortHttpParams = Utils.parseURL("jdbc:hive2://hello.host/default;transportMode=http");
    Assert.assertEquals(10001, nonPortHttpParams.getPort());
  }

  @Test
  public void testPasswordExtractionFromUrl() throws SQLException, ZooKeeperHiveClientException {
    // Test with password in the URL
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=mySecretPassword;transportMode=http");

    Assert.assertEquals("mySecretPassword",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));

    // Test with password in different case (uppercase)
    params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;PASSWORD=upperCasePassword;httpPath=cliservice");

    Assert.assertEquals("upperCasePassword",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));

    // Test with password at the beginning of parameters
    params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=firstParam;httpPath=cliservice");

    Assert.assertEquals("firstParam",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));

    // Test with password at the end of parameters
    params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;transportMode=http;password=lastParam");

    Assert.assertEquals("lastParam",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));
  }

  @Test
  public void testPasswordWithSpecialCharacters() throws SQLException, ZooKeeperHiveClientException {
    // Test with password containing special characters
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=Pass@123!#$;httpPath=cliservice");

    Assert.assertEquals("Pass@123!#$",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));

    // Test with password containing equals sign
    params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=user=admin123;transportMode=http");

    Assert.assertEquals("user=admin123",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));
  }

  @Test
  public void testNoPasswordInUrl() throws SQLException, ZooKeeperHiveClientException {
    // Test URL without password parameter
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;transportMode=http;httpPath=cliservice");

    Assert.assertNull(params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));
  }

  @Test
  public void testEmptyPassword() throws SQLException, ZooKeeperHiveClientException {
    // Test with empty password
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=;transportMode=http");

    Assert.assertEquals("",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));
  }

  @Test
  public void testPasswordWithSemicolon() throws SQLException, ZooKeeperHiveClientException {
    // Test URL with semicolon in password (this should extract up to the semicolon)
    JdbcConnectionParams params = Utils.parseURL(
        "jdbc:hive2://hello.host:10002/default;password=part1;part2;transportMode=http");

    Assert.assertEquals("part1",
        params.getSessionVars().get(JdbcConnectionParams.AUTH_PASSWD));
  }
}
