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
package org.apache.hive.service.auth;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.sasl.AuthenticationException;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class TestCustomAuthentication {

  private static HiveServer2 hiveserver2 = null;

  private static File configFile = null;

  @BeforeClass
  public static void setUp() throws Exception {
    createConfig();
    startServer();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    stopServer();
    removeConfig();
  }

  private static void startServer() throws Exception{

    HiveConf hiveConf = new HiveConf();
    hiveserver2 = new HiveServer2();
    hiveserver2.init(hiveConf);
    hiveserver2.start();
    Thread.sleep(1000);
    System.out.println("hiveServer2 start ......");

  }

  private static void stopServer(){
    try {
      if (hiveserver2 != null) {
        hiveserver2.stop();
        hiveserver2 = null;
      }
      Thread.sleep(1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("hiveServer2 stop ......");
  }

  private static void createConfig() throws Exception{

    Configuration conf = new Configuration(false);
    conf.set("hive.server2.authentication", "CUSTOM");
    conf.set("hive.server2.custom.authentication.class",
        "org.apache.hive.service.auth.TestCustomAuthentication$SimpleAuthenticationProviderImpl");

    configFile = new File("../build/service/test/resources","hive-site.xml");

    FileOutputStream out = new FileOutputStream(configFile);
    conf.writeXml(out);
  }

  private static void removeConfig(){
    try {
      configFile.delete();
    } catch (Exception e){
      System.out.println(e.getMessage());
    }
  }

  @Test
  public void testCustomAuthentication() throws Exception{

    String url = "jdbc:hive2://localhost:10000/default";

    Exception exception = null;
    try{
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Connection connection =  DriverManager.getConnection(url, "wronguser", "pwd");
      connection.close();
    } catch (Exception e){
      exception = e;
    }

    Assert.assertNotNull(exception);

    exception = null;
    try{
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      Connection connection =  DriverManager.getConnection(url, "hiveuser", "hive");
      connection.close();
    } catch (Exception e){
      exception = e;
    }

    Assert.assertNull(exception);

    System.out.println(">>> PASSED testCustomAuthentication");
  }

  public static class SimpleAuthenticationProviderImpl implements PasswdAuthenticationProvider {

    private Map<String, String> userMap = new HashMap<String, String>();

    public SimpleAuthenticationProviderImpl() {
      init();
    }

    private void init(){
      userMap.put("hiveuser","hive");
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
