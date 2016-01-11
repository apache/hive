/**
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

package org.apache.hive.service.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestHS2HttpServer -- executes tests of HiveServer2 HTTP Server
 */
public class TestHS2HttpServer {

  private static HiveServer2 hiveServer2 = null;

  @BeforeClass
  public static void beforeTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, false);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(5000);
  }

  @Test
  public void testStackServket() throws Exception {
    String baseURL = "http://localhost:"
      + ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue() + "/stacks";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream()));
    boolean contents = false;
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.contains("Process Thread Dump:")) {
        contents = true;
      }
    }
    Assert.assertTrue(contents);
  }

  @AfterClass
  public static void afterTests() throws Exception {
    hiveServer2.stop();
  }
}
