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

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStandaloneJdbc {
  private static File workDir = new File(createTempDir(), "test-it-standalone-jdbc");
  private static ITAbstractContainer HS2;
  private static ITAbstractContainer ZK_HS2;

  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    String baseName = System.currentTimeMillis() + "-";

    for (int counter = 0; counter < 100; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdir()) {
        return tempDir;
      }
    }
    throw new IllegalStateException(
        "Failed to create directory within 100 attempts (tried "
            + baseName
            + "0 to "
            + baseName
            + 99
            + ')');
  }

  @BeforeClass
  public static void setup() throws Exception {
    workDir.mkdirs();
    HS2 = new ITHiveServer2(workDir);
    ZK_HS2 = new ITZKHiveServer2(workDir);

    HS2.start();
    ZK_HS2.start();
  }

  @Test
  public void testBinaryJdbc() throws Exception {
    testMetaOp(HS2.getBaseJdbcUrl(), true);
    testDataSourceOp(HS2.getBaseJdbcUrl());
    testNegativeJdbc(HS2.getBaseJdbcUrl());
  }

  private void testMetaOp(String url, boolean kerberos) throws Exception {
    try (Connection con = DriverManager.getConnection(url)) {
      try (Statement stmt = con.createStatement()) {
        String tableName = "testHiveDriverTable1";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key string, value string)");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
          System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // "Client cannot authenticate via:[TOKEN, KERBEROS]" in running the Kerberized tez local mode
        if (!kerberos) {
          String values = "('a','b'),('c','d'),('e','f'),('g','h')";
          stmt.execute("insert into table " + tableName + " values" + values);
          // select * query
          sql = "select * from " + tableName;
          System.out.println("Running: " + sql);
          res = stmt.executeQuery(sql);
          List<String> list = new ArrayList<>();
          while (res.next()) {
            list.add("('" + res.getString(1) + "','" + res.getString(2) + "')");
          }
          assertEquals(values, list.stream().collect(Collectors.joining(",")));

          // regular hive query
          sql = "select count(1) from " + tableName;
          System.out.println("Running: " + sql);
          res = stmt.executeQuery(sql);
          assertTrue(res.next());
          assertEquals(4, res.getInt(1));
        }
      }
    }
  }

  private void testDataSourceOp(String url) throws Exception {
    try (Connection con = DriverManager.getConnection(url)) {
      DatabaseMetaData metaData = con.getMetaData();
      ResultSet rs = metaData.getSchemas();
      assertTrue(rs.next());
      while (rs.next()) {
        System.out.println("DatabaseMetaData.getSchemas: " + rs.getString(1));
      }
      ResultSet resultSet = metaData.getColumns("hive", ".*", ".*", ".*");
      assertTrue(resultSet.next());
      while (resultSet.next()) {
        System.out.println("DatabaseMetaData.getColumns: " + rs.getString(1));
      }
      resultSet = metaData.getTypeInfo();
      assertTrue(resultSet.next());
      while (resultSet.next()) {
        System.out.println("DatabaseMetaData.getTypeInfo: " + rs.getString(1));
      }
    }
  }

  @Test
  public void testHttpJdbc() throws Exception {
    testMetaOp(HS2.getHttpJdbcUrl(), true);
    testMetaOp(ZK_HS2.getHttpJdbcUrl(), false);
    testDataSourceOp(HS2.getHttpJdbcUrl());
    testNegativeJdbc(HS2.getHttpJdbcUrl());
  }

  private void testNegativeJdbc(String url) throws Exception {
    Connection con = DriverManager.getConnection(url);
    try {
      Statement stmt = con.createStatement();
      stmt.execute("insert into table this_is_not_exist_table values (1),(3),(5)");
      fail("A SQLException is expected");
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      con.close();
    }
  }

  @Test
  public void testZookeeperJdbc() throws Exception {
    testMetaOp(ZK_HS2.getZkConnectionUrl(), false);
    testDataSourceOp(ZK_HS2.getZkConnectionUrl());
    testNegativeJdbc(ZK_HS2.getZkConnectionUrl());
  }

  @Test
  public void testTokenAuthentication() throws Exception {
    String url = HS2.getBaseJdbcUrl();
    String dt;
    try (HiveConnection con = (HiveConnection) DriverManager.getConnection(url)) {
      dt = con.getDelegationToken(ITHiveServer2.MiniJdbcKdc.HIVE_TEST_USER_1, "hive");
    }
    File dtFile = new File(workDir, "delegation-token-file");
    try (FileOutputStream os = new FileOutputStream(dtFile)) {
      os.write(dt.getBytes());
    }
    SecurityUtils.setTokenStr(UserGroupInformation.getCurrentUser(), dt, "hiveserver2ClientToken");
    //  HADOOP_TOKEN_FILE_LOCATION
    updateEnv("HADOOP_TOKEN_FILE_LOCATION", dtFile.getPath());
    url += ";auth=delegationToken";
    testMetaOp(url, true);
  }

  public void updateEnv(String name, String val) throws ReflectiveOperationException {
    Map<String, String> env = System.getenv();
    Field field = env.getClass().getDeclaredField("m");
    field.setAccessible(true);
    ((Map<String, String>) field.get(env)).put(name, val);
  }

  @AfterClass
  public static void destroy() throws Exception {
    try {
      workDir.delete();
    } finally {
      killContainer(ZK_HS2, HS2);
    }
  }

  private static void killContainer(ITAbstractContainer... containers) {
    for (ITAbstractContainer container : containers) {
      try {
        if (container != null) {
          container.stop();
        }
      } catch (Exception e) {
        // ignore this exception
      }
    }
  }
}
