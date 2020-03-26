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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * TestBeelineConnectionUsingHiveSite test.
 */
@RunWith(Parameterized.class)
public class TestBeelineConnectionUsingHiveSite extends BeelineWithHS2ConnectionFileTestBase {

  boolean isHttpModeTest = false;

  @Parameterized.Parameters(name = "{index}: tranportMode={0}")
  public static Collection<Object[]> transportModes() {
    return Arrays.asList(new Object[][]{{MiniHS2.HS2_BINARY_MODE}, {MiniHS2.HS2_HTTP_MODE}, {MiniHS2.HS2_ALL_MODE}});
  }
  @Test
  public void testBeelineConnectionHttp() throws Exception {
    Assume.assumeTrue(transportMode.equals(MiniHS2.HS2_HTTP_MODE)
        || transportMode.equalsIgnoreCase(MiniHS2.HS2_ALL_MODE));
    isHttpModeTest = true;
    setupHs2();
    String path = createDefaultHs2ConnectionFile();
    assertBeelineOutputContains(path, new String[] { "-e", "show tables;" }, tableName);
    isHttpModeTest = false;
  }

  @Test
  public void testBeelineConnectionSSL() throws Exception {
    Assume.assumeTrue(transportMode.equals(MiniHS2.HS2_BINARY_MODE)
        || transportMode.equalsIgnoreCase(MiniHS2.HS2_ALL_MODE));
    setupSSLHs2();
    String path = createDefaultHs2ConnectionFile();
    assertBeelineOutputContains(path, new String[] { "-e", "show tables;" }, tableName);
  }

  @Test
  public void testBeelineConnectionNoAuth() throws Exception {
    Assume.assumeTrue(transportMode.equals(MiniHS2.HS2_BINARY_MODE)
        || transportMode.equalsIgnoreCase(MiniHS2.HS2_ALL_MODE));
    setupNoAuthHs2();
    String path = createDefaultHs2ConnectionFile();
    assertBeelineOutputContains(path, new String[] { "-e", "show tables;" }, tableName);
  }

  @Test
  public void testBeelineDoesntUseDefaultIfU() throws Exception {
    setupNoAuthHs2();
    String path = createDefaultHs2ConnectionFile();
    BeelineResult res = getBeelineOutput(path, new String[] {"-u", "invalidUrl", "-e", "show tables;" });
    assertEquals(1, res.exitCode);
    assertFalse(tableName + " should not appear", res.output.toLowerCase().contains(tableName));

  }

  /*
   * tests if the beeline behaves like default mode if there is no user-specific connection
   * configuration file
   */
  @Test
  public void testBeelineWithNoConnectionFile() throws Exception {
    Assume.assumeTrue(transportMode.equals(MiniHS2.HS2_BINARY_MODE)
        || transportMode.equalsIgnoreCase(MiniHS2.HS2_ALL_MODE));
    setupNoAuthHs2();
    BeelineResult res = getBeelineOutput(null, new String[] {"-e", "show tables;" });
    assertEquals(1, res.exitCode);
    assertTrue(res.output.toLowerCase().contains("no current connection"));
  }

  @Test
  public void testBeelineUsingArgs() throws Exception {
    Assume.assumeTrue(transportMode.equals(MiniHS2.HS2_BINARY_MODE)
        || transportMode.equalsIgnoreCase(MiniHS2.HS2_ALL_MODE));
    setupNoAuthHs2();
    String url = miniHS2.getBaseJdbcURL() + "default";
    String args[] = new String[] { "-u", url, "-n", System.getProperty("user.name"), "-p", "foo",
        "-e", "show tables;" };
    assertBeelineOutputContains(null, args, tableName);
  }

  private void setupNoAuthHs2() throws Exception {
    // use default configuration for no-auth mode
    miniHS2.start(confOverlay);
    createTable();
  }

  private void setupSSLHs2() throws Exception {
    confOverlay.put(ConfVars.HIVE_SERVER2_USE_SSL.varname, "true");
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname,
        dataFileDir + File.separator + LOCALHOST_KEY_STORE_NAME);
    confOverlay.put(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname,
        KEY_STORE_TRUST_STORE_PASSWORD);
    miniHS2.start(confOverlay);
    createTable();
    System.setProperty(JAVA_TRUST_STORE_PROP, dataFileDir + File.separator + TRUST_STORE_NAME);
    System.setProperty(JAVA_TRUST_STORE_PASS_PROP, KEY_STORE_TRUST_STORE_PASSWORD);
  }

  private void setupHs2() throws Exception {
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
    miniHS2.start(confOverlay);
    createTable();
  }

  private String createDefaultHs2ConnectionFile() throws Exception {
    Hs2ConnectionXmlConfigFileWriter writer = new Hs2ConnectionXmlConfigFileWriter();
    String baseJdbcURL = miniHS2.getBaseJdbcURL();
    if(isHttpModeTest) {
      baseJdbcURL = miniHS2.getBaseHttpJdbcURL();
    }
    System.out.println(baseJdbcURL);
    writer.writeProperty(HS2ConnectionFileParser.BEELINE_CONNECTION_PROPERTY_PREFIX + "user",
        System.getProperty("user.name"));
    writer.writeProperty(HS2ConnectionFileParser.BEELINE_CONNECTION_PROPERTY_PREFIX + "password", "foo");
    writer.close();
    return writer.path();
  }
}
