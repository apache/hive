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

package org.apache.hive.minikdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

/**
 * TestSuite to test Hive's LDAP Authentication provider with an
 * in-process LDAP Server (Apache Directory Server instance).
 */
@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {
    @CreateTransport(protocol = "LDAP", port = 10390 ),
    @CreateTransport(protocol = "LDAPS", port = 10640 )
})

@CreateDS(partitions = {
    @CreatePartition(
        name = "example",
        suffix = "dc=example,dc=com",
        contextEntry = @ContextEntry(entryLdif =
            "dn: dc=example,dc=com\n" +
                "dc: example\n" +
                "objectClass: top\n" +
                "objectClass: domain\n\n"
        ),
        indexes = {
            @CreateIndex(attribute = "objectClass"),
            @CreateIndex(attribute = "cn"),
            @CreateIndex(attribute = "uid")
        }
    )
})

@ApplyLdifFiles({
    "ldap/example.com.ldif",
    "ldap/microsoft.schema.ldif",
    "ldap/ad.example.com.ldif"
})
// Test HS2 with Kerberos + LDAP auth methods
public class TestHS2AuthMechsWithMiniKdc extends AbstractLdapTestUnit {
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;

  @Before
  public void setUpBefore() throws Exception {
    if (miniHS2 == null) {
      Class.forName(MiniHS2.getJdbcDriverName());
      miniHiveKdc = new MiniHiveKdc();
      HiveConf hiveConf = new HiveConf();
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL,
          "ldap://localhost:" + ldapServer.getPort());
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
          "uid=%s,ou=People,dc=example,dc=com");

      AuthenticationProviderFactory.AuthMethods.LDAP.getConf().setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL,
          "ldap://localhost:" + ldapServer.getPort());
      AuthenticationProviderFactory.AuthMethods.LDAP.getConf().setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
          "uid=%s,ou=People,dc=example,dc=com");
      miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf,
          HiveAuthConstants.AuthTypes.KERBEROS.getAuthName() + "," + HiveAuthConstants.AuthTypes.LDAP.getAuthName());
      miniHS2.getHiveConf().setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, MiniHS2.HS2_ALL_MODE);
      miniHS2.start(new HashMap<>());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ldapServer.isStarted()) {
      ldapServer.stop();
    }
    miniHS2.stop();
  }

  private void testKrbPasswordAuth(boolean httpMode) throws Exception {
    String baseJdbc, jdbc;
    if (!httpMode) {
      baseJdbc = miniHS2.getBaseJdbcURL() + "default;";
      jdbc = miniHS2.getJdbcURL();
    } else {
      baseJdbc = miniHS2.getBaseHttpJdbcURL() + "default;transportMode=http;httpPath=cliservice;";
      jdbc = miniHS2.getHttpJdbcURL();
    }

    // First we try logging through Kerberos
    try {
      String principle = miniHiveKdc.getFullyQualifiedServicePrincipal("dummy_user");
      DriverManager.getConnection(baseJdbc + "default;principal=" + principle);
      fail("Should fail to establish the connection as server principle is wrong");
    } catch (Exception e) {
      if (!httpMode) {
        Assert.assertTrue(e.getMessage().contains("GSS initiate failed"));
      } else {
        Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("Failed to find any Kerberos ticket"));
      }
    }

    try (Connection hs2Conn = DriverManager.getConnection(jdbc)) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("create table if not exists test_hs2_with_multiple_auths(a string)");
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }

    // Next, test logging through LDAP
    try {
      DriverManager.getConnection(baseJdbc + "user=user1;password=password");
      fail("Should fail to establish the connection as password is wrong");
    } catch (Exception e) {
      if (!httpMode) {
        Assert.assertTrue(e.getMessage().contains("Error validating the login"));
      } else {
        Assert.assertTrue(e.getMessage().contains("HTTP Response code: 401"));
      }
    }

    try (Connection hs2Conn = DriverManager.getConnection(baseJdbc + "user=user2;password=user2")) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }
  }

  @Test
  public void testKrbPasswordAuth() throws Exception {
    // Test the binary mode
    testKrbPasswordAuth(false);
    // Test the http mode
    testKrbPasswordAuth(true);
  }

  private void validateResult(ResultSet rs, int expectedSize) throws Exception {
    int actualSize = 0;
    while (rs.next()) {
      actualSize ++;
    }
    Assert.assertEquals(expectedSize, actualSize);
  }

}
