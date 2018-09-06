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
package org.apache.hive.beeline.hs2connection;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hive.beeline.hs2connection.BeelineHS2ConnectionFileParseException;
import org.apache.hive.beeline.hs2connection.UserHS2ConnectionFileParser;
import org.apache.hive.beeline.hs2connection.HS2ConnectionFileUtils;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestUserHS2ConnectionFileParser {
  private final String LOCATION_1 = System.getProperty("java.io.tmpdir") + "loc1" + File.separator
      + UserHS2ConnectionFileParser.DEFAULT_CONNECTION_CONFIG_FILE_NAME;

  private final String LOCATION_2 = System.getProperty("java.io.tmpdir") + "loc2" + File.separator
      + UserHS2ConnectionFileParser.DEFAULT_CONNECTION_CONFIG_FILE_NAME;

  private final String LOCATION_3 = System.getProperty("java.io.tmpdir") + "loc3" + File.separator
      + UserHS2ConnectionFileParser.DEFAULT_CONNECTION_CONFIG_FILE_NAME;

  List<String> testLocations = new ArrayList<>();

  @After
  public void cleanUp() {
    try {
      deleteFile(LOCATION_1);
      deleteFile(LOCATION_2);
      deleteFile(LOCATION_3);
    } catch (Exception e) {
      e.printStackTrace();
    }
    testLocations.clear();
  }

  @Test
  public void testParseNoAuthentication() throws BeelineHS2ConnectionFileParseException {
    String url = getParsedUrlFromConfigFile("test-hs2-connection-config-noauth.xml");
    String expectedUrl = "jdbc:hive2://localhost:10000/default;user=hive";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testParseZookeeper() throws BeelineHS2ConnectionFileParseException {
    String url = getParsedUrlFromConfigFile("test-hs2-connection-zookeeper-config.xml");
    String expectedUrl =
        "jdbc:hive2://zk-node-1:10000,zk-node-2:10001,zk-node-3:10004/default;serviceDiscoveryMode=zookeeper;zooKeeperNamespace=hiveserver2";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testParseWithKerberosNoSSL() throws BeelineHS2ConnectionFileParseException {
    String url = getParsedUrlFromConfigFile("test-hs2-conn-conf-kerberos-nossl.xml");
    String expectedUrl =
        "jdbc:hive2://localhost:10000/default;principal=hive/dummy-hostname@domain.com;ssl=false";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testParseWithKerberosSSL() throws BeelineHS2ConnectionFileParseException {
    String url = getParsedUrlFromConfigFile("test-hs2-conn-conf-kerberos-ssl.xml");
    String expectedUrl =
        "jdbc:hive2://localhost:10000/default;principal=hive/dummy-hostname@domain.com;ssl=true;"
            + "sslTrustStore=test/truststore;trustStorePassword=testTruststorePassword";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testParseWithSSLAndHttpMode() throws BeelineHS2ConnectionFileParseException {
    String url = getParsedUrlFromConfigFile("test-hs2-conn-conf-kerberos-http.xml");
    String expectedUrl =
        "jdbc:hive2://localhost:10000/default;httpPath=testHTTPPath;principal=hive/dummy-hostname@domain.com;"
            + "ssl=true;sslTrustStore=test/truststore;transportMode=http;trustStorePassword=testTruststorePassword";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testUrlWithHiveConfValues() throws Exception {
    String url = getParsedUrlFromConfigFile("test-hs2-connection-conf-list.xml");
    String expectedUrl =
        "jdbc:hive2://localhost:10000/default;user=hive?hive.cli.print.current.db=false#testVarName1=value1";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  @Test
  public void testUrlWithMultipleHiveConfValues() throws Exception {
    String url = getParsedUrlFromConfigFile("test-hs2-connection-multi-conf-list.xml");
    String expectedUrl =
        "jdbc:hive2://localhost:10000/default;user=hive?hive.cli.print.current.db=true;"
            + "hive.cli.print.header=true#testVarName1=value1;testVarName2=value2";
    Assert.assertTrue("Expected " + expectedUrl + " got " + url, expectedUrl.equals(url));
  }

  /*
   * Tests if null value returned when file is not present in any of the lookup locations
   */
  @Test
  public void testNoLocationFoundCase() throws Exception {
    testLocations.add(LOCATION_1);
    testLocations.add(LOCATION_2);
    testLocations.add(LOCATION_3);
    UserHS2ConnectionFileParser testHS2ConfigManager =
        new UserHS2ConnectionFileParser(testLocations);
    Assert.assertTrue(testHS2ConfigManager.getConnectionProperties().isEmpty());
  }

  /*
   * Tests if LOCATION_1 is returned when file is present in the first directory in lookup order
   */
  @Test
  public void testGetLocation1() throws Exception {
    createNewFile(LOCATION_1);
    testLocations.add(LOCATION_1);
    testLocations.add(LOCATION_2);
    testLocations.add(LOCATION_3);
    UserHS2ConnectionFileParser testHS2ConfigManager =
        new UserHS2ConnectionFileParser(testLocations);
    Assert.assertTrue("File location " + LOCATION_1 + " was not returned",
        LOCATION_1.equals(testHS2ConfigManager.getFileLocation()));
  }

  /*
   * Tests if LOCATION_3 is returned when the first file is found is later in lookup order
   */
  @Test
  public void testGetLocation3() throws Exception {
    createNewFile(LOCATION_3);
    testLocations.add(LOCATION_1);
    testLocations.add(LOCATION_2);
    testLocations.add(LOCATION_3);
    UserHS2ConnectionFileParser testHS2ConfigManager =
        new UserHS2ConnectionFileParser(testLocations);
    Assert.assertTrue("File location " + LOCATION_3 + " was not returned",
        LOCATION_3.equals(testHS2ConfigManager.getFileLocation()));
  }

  /*
   * Tests if it returns the first file present in the lookup order when files are present in the
   * lookup order
   */
  @Test
  public void testGetLocationOrder() throws Exception {
    createNewFile(LOCATION_2);
    createNewFile(LOCATION_3);
    testLocations.add(LOCATION_1);
    testLocations.add(LOCATION_2);
    testLocations.add(LOCATION_3);
    UserHS2ConnectionFileParser testHS2ConfigManager =
        new UserHS2ConnectionFileParser(testLocations);
    Assert.assertTrue("File location " + LOCATION_2 + " was not returned",
        LOCATION_2.equals(testHS2ConfigManager.getFileLocation()));
  }

  @Test
  public void testConfigLocationPathInEtc() throws Exception {
    UserHS2ConnectionFileParser testHS2ConfigManager =
            new UserHS2ConnectionFileParser();
    Field locations = testHS2ConfigManager.getClass().getDeclaredField("locations");
    locations.setAccessible(true);
    Collection<String> locs = (List<String>)locations.get(testHS2ConfigManager);
    Assert.assertTrue(locs.contains(
            UserHS2ConnectionFileParser.ETC_HIVE_CONF_LOCATION +
            File.separator +
            UserHS2ConnectionFileParser.DEFAULT_CONNECTION_CONFIG_FILE_NAME));

  }

  private String getParsedUrlFromConfigFile(String filename)
      throws BeelineHS2ConnectionFileParseException {
    String path = HiveTestUtils.getFileFromClasspath(filename);
    testLocations.add(path);
    UserHS2ConnectionFileParser testHS2ConfigManager =
        new UserHS2ConnectionFileParser(testLocations);
    return HS2ConnectionFileUtils.getUrl(testHS2ConfigManager.getConnectionProperties());
  }

  private void createNewFile(final String path) throws Exception {
    File file = new File(path);
    if (file.exists()) {
      return;
    }
    String dir = path.substring(0,
        path.indexOf(UserHS2ConnectionFileParser.DEFAULT_CONNECTION_CONFIG_FILE_NAME));
    if (!new File(dir).exists()) {
      if (!new File(dir).mkdirs()) {
        throw new Exception("Could not create directory " + dir);
      }
    }
    if (!file.createNewFile()) {
      throw new Exception("Could not create new file at " + path);
    }
  }

  private void deleteFile(final String path) throws Exception {
    File file = new File(path);
    if (file.exists()) {
      if (!file.delete()) {
        throw new Exception("Could not delete file at " + path);
      }
    }
  }

}
