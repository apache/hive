/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRestrictedList {
  private static MiniHS2 miniHS2 = null;
  private static URL oldHiveSiteURL = null;
  private static URL oldHiveMetastoreSiteURL = null;
  private static Map<String, String> expectedRestrictedMap = new HashMap<>();
  private static HiveConf hiveConf = null;

  @BeforeClass
  public static void startServices() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    oldHiveSiteURL = HiveConf.getHiveSiteLocation();
    oldHiveMetastoreSiteURL = HiveConf.getMetastoreSiteLocation();
    String confDir = "../../data/conf/rlist/";
    HiveConf.setHiveSiteLocation(
        new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    HiveConf.setHivemetastoreSiteUrl(
        new URL("file://" + new File(confDir).toURI().getPath() + "/hivemetastore-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    hiveConf = new HiveConf();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS, 1);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS, 1);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    miniHS2 = new MiniHS2.Builder().withMiniMR().withRemoteMetastore().withConf(hiveConf).build();
    miniHS2.start(new HashMap<String, String>());

    // Add the parameter here if it cannot change at runtime
    addToExpectedRestrictedMap("hive.conf.restricted.list");
    addToExpectedRestrictedMap("hive.security.authenticator.manager");
    addToExpectedRestrictedMap("hive.security.authorization.manager");
    addToExpectedRestrictedMap("hive.security.metastore.authorization.manager");
    addToExpectedRestrictedMap("hive.security.metastore.authenticator.manager");
    addToExpectedRestrictedMap("hive.users.in.admin.role");
    addToExpectedRestrictedMap("hive.server2.xsrf.filter.enabled");
    addToExpectedRestrictedMap("hive.security.authorization.enabled");
    addToExpectedRestrictedMap("hive.distcp.privileged.doAs");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.baseDN");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.url");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.Domain");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.groupDNPattern");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.groupFilter");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.userDNPattern");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.userFilter");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.groupMembershipKey");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.userMembershipKey");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.groupClassKey");
    addToExpectedRestrictedMap("hive.server2.authentication.ldap.customLDAPQuery");
    addToExpectedRestrictedMap("hive.spark.client.channel.log.level");
    addToExpectedRestrictedMap("hive.spark.client.secret.bits");
    addToExpectedRestrictedMap("hive.spark.client.rpc.server.address");
    addToExpectedRestrictedMap("hive.spark.client.rpc.server.port");
    addToExpectedRestrictedMap("hive.spark.client.rpc.sasl.mechanisms");
    addToExpectedRestrictedMap("bonecp.test");
    addToExpectedRestrictedMap("hive.druid.broker.address.default");
    addToExpectedRestrictedMap("hive.druid.coordinator.address.default");
    addToExpectedRestrictedMap("hikari.test");
    addToExpectedRestrictedMap("hadoop.bin.path");
    addToExpectedRestrictedMap("yarn.bin.path");
    addToExpectedRestrictedMap("hive.spark.client.connect.timeout");
    addToExpectedRestrictedMap("hive.spark.client.server.connect.timeout");
    addToExpectedRestrictedMap("hive.spark.client.rpc.max.size");
    addToExpectedRestrictedMap("hive.spark.client.rpc.threads");
    addToExpectedRestrictedMap("_hive.local.session.path");
    addToExpectedRestrictedMap("_hive.tmp_table_space");
    addToExpectedRestrictedMap("_hive.hdfs.session.path");
    addToExpectedRestrictedMap("hive.spark.client.rpc.server.address");
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    HiveConf.setHivemetastoreSiteUrl(oldHiveMetastoreSiteURL);
    HiveConf.setHiveSiteLocation(oldHiveSiteURL);
  }

  @Test
  public void testRestrictedList() throws Exception {
    assertTrue("Test setup failed. MiniHS2 is not initialized",
        miniHS2 != null && miniHS2.isStarted());

    checkRestrictedListMatch();

    try (Connection hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "hive", "hive");
         Statement stmt = hs2Conn.createStatement();) {
      for (Map.Entry<String, String> entry : expectedRestrictedMap.entrySet()) {
        String parameter = entry.getKey();
        String value = entry.getValue();

        try {
          stmt.execute("set " + parameter + "=" + value);
          fail("Exception not thrown for parameter: " + parameter);
        } catch (Exception e1) {
          assertTrue("Unexpected exception: " + e1.getMessage(),
              e1.getMessage().contains("Error while processing statement: Cannot modify"));
        }
      }
    } catch (Exception e2) {
      fail("Unexpected Exception: " + e2.getMessage());
    }
  }

  // This test will make sure that every entry in hive.conf.restricted.list, has a test here
  private void checkRestrictedListMatch(){
    HiveConf.ConfVars restrictedConfVar = HiveConf.getConfVars("hive.conf.restricted.list");
    String definedRestrictedListString = HiveConf.getVar(hiveConf, restrictedConfVar);
    Set<String> definedRestrictedSet = new HashSet<String>();

    definedRestrictedSet.clear();
    assertTrue(definedRestrictedListString != null);

    // populate definedRestrictedSet with parameters defined in hive.conf.restricted.list
    for (String entry : definedRestrictedListString.split(",")) {
      definedRestrictedSet.add(entry.trim());
    }

    // remove all parameters that are tested.  if the parameter is tested it is part of
    // expectedRestrictedMap
    definedRestrictedSet.removeAll(expectedRestrictedMap.keySet());

    // the remaining parameters in definedRestrictedSet are starting parameter name
    for (String definedRestrictedParameter : definedRestrictedSet) {
      boolean definedRestrictedParameterTested = false;
      for (String expectedRestrictedParameter : expectedRestrictedMap.keySet()) {
        if (expectedRestrictedParameter.startsWith(definedRestrictedParameter)) {
          definedRestrictedParameterTested = true;
          break;
        }
      }
      assertTrue(definedRestrictedParameter + " not tested.", definedRestrictedParameterTested);
    }
  }

  private static void addToExpectedRestrictedMap(String parameter) {
    HiveConf.ConfVars confVars = HiveConf.getConfVars(parameter);
    String value = "foo";

    if (confVars != null) {
      if (confVars.isType("foo") && confVars.validate("foo") == null) {
        value = "foo";
      } else if (confVars.isType("1s") && confVars.validate("1s") == null) {
        value = "1s";
      } else if (confVars.isType("1") && confVars.validate("1") == null) {
        value = "1";
      }
    }
    expectedRestrictedMap.put(parameter, value);
  }
}
