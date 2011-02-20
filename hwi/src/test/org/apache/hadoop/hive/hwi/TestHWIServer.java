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
package org.apache.hadoop.hive.hwi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.hive.shims.JettyShims;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * TestHWIServer.
 *
 */
public class TestHWIServer extends TestCase {

  public TestHWIServer(String name) {
    super(name);

  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

  }

  public final void testServerInit() throws Exception {
    StringBuilder warFile = new StringBuilder("../build/hwi/hive-hwi-");
    Properties props = new Properties();

    // try retrieve version from build.properties file
    try {
      props.load(new FileInputStream("../build.properties"));
      warFile.append(props.getProperty("version")).append(".war");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    JettyShims.Server webServer;
    webServer = ShimLoader.getJettyShims().startServer("0.0.0.0", 9999);
    assertNotNull(webServer);
    webServer.addWar(warFile.toString(), "/hwi");
    webServer.start();
    // webServer.join();
    webServer.stop();
    assert (true);
  }

}
