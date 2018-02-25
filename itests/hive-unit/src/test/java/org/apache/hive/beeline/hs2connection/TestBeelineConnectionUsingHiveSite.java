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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Test;

public class TestBeelineConnectionUsingHiveSite extends BeelineWithHS2ConnectionFileTestBase {
  @Test
  public void testBeelineConnectionHttp() throws Exception {
    setupHs2();
    String path = createDefaultHs2ConnectionFile();
    testBeeLineConnection(path, new String[] { "-e", "show tables;" }, tableName);
  }

  @Test
  public void testBeelineConnectionSSL() throws Exception {
    setupSSLHs2();
    String path = createDefaultHs2ConnectionFile();
    testBeeLineConnection(path, new String[] { "-e", "show tables;" }, tableName);
  }

  @Test
  public void testBeelineConnectionNoAuth() throws Exception {
    setupNoAuthHs2();
    String path = createDefaultHs2ConnectionFile();
    testBeeLineConnection(path, new String[] { "-e", "show tables;" }, tableName);
  }

  /*
   * tests if the beeline behaves like default mode if there is no user-specific connection
   * configuration file
   */
  @Test
  public void testBeelineWithNoConnectionFile() throws Exception {
    setupNoAuthHs2();
    testBeeLineConnection(null, new String[] { "-e", "show tables;" }, "no current connection");
  }

  @Test
  public void testBeelineUsingArgs() throws Exception {
    setupNoAuthHs2();
    String url = miniHS2.getBaseJdbcURL() + "default";
    String args[] = new String[] { "-u", url, "-n", System.getProperty("user.name"), "-p", "foo",
        "-e", "show tables;" };
    testBeeLineConnection(null, args, tableName);
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
    confOverlay.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, HS2_HTTP_MODE);
    confOverlay.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, HS2_HTTP_ENDPOINT);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "true");
    miniHS2.start(confOverlay);
    createTable();
  }

  private String createDefaultHs2ConnectionFile() throws Exception {
    Hs2ConnectionXmlConfigFileWriter writer = new Hs2ConnectionXmlConfigFileWriter();
    String baseJdbcURL = miniHS2.getBaseJdbcURL();
    System.out.println(baseJdbcURL);
    writer.writeProperty(HS2ConnectionFileParser.BEELINE_CONNECTION_PROPERTY_PREFIX + "user",
        System.getProperty("user.name"));
    writer.writeProperty(HS2ConnectionFileParser.BEELINE_CONNECTION_PROPERTY_PREFIX + "password", "foo");
    writer.close();
    return writer.path();
  }
}
