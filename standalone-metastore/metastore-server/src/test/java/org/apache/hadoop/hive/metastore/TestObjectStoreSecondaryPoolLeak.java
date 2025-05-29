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
package org.apache.hadoop.hive.metastore;

import com.github.zabetak.jdbc.faulty.DelayFault;
import com.github.zabetak.jdbc.faulty.FaultyJDBCDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

@Category(MetastoreCheckinTest.class)
public class TestObjectStoreSecondaryPoolLeak {
  private static final DatabaseRule DBMS = new Mysql();

  @BeforeAll
  static void setup() throws Exception {
    DBMS.before();
    DBMS.install();
    try (Connection connection = DriverManager.getConnection(DBMS.getJdbcUrl(), DBMS.getDbRootUser(),
        DBMS.getDbRootPassword())) {
      Statement stmt = connection.createStatement();
      stmt.execute("SET GLOBAL wait_timeout=2");
    }
  }

  @AfterAll
  static void teardown() throws Exception {
    FaultyJDBCDriver.clearFaults();
    DBMS.after();
  }

  private static ObjectStore create() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    conf.set("hikaricp.connectionTimeout", "500");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, "HikariCP");
    MetastoreConf.setLongVar(conf, ConfVars.CONNECTION_POOLING_MAX_SECONDARY_CONNECTIONS, 2);
    MetastoreConf.setVar(conf, ConfVars.CONNECTION_DRIVER, DBMS.getJdbcDriver());
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, DBMS.getJdbcUrl());
    MetastoreConf.setVar(conf, ConfVars.PWD, DBMS.getHivePassword());
    MetastoreConf.setVar(conf, ConfVars.CONNECTION_USER_NAME, DBMS.getHiveUser());
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, DBMS.getJdbcUrl().replace("jdbc:", "jdbc:faulty:"));
    ObjectStore store = new ObjectStore();
    store.setConf(conf);
    return store;
  }


  @Test
  public void testCreateTableWithRandomTimeoutOnCommit() throws Exception {
    ObjectStore store = create();
    store.createDatabase(
        new DatabaseBuilder().setName("default").setLocation("file:///path/to/default").build(store.getConf()));
    // Add a fault creating a delay that is bigger than the wait_timeout of the underlying database
    // in 8% of the Connection#commit calls that will eventually cause the operation to fail.
    FaultyJDBCDriver.addFault("f1", new DelayFault(0.08,"commit", 3000));
    // Since the faults are probabilistic, it is not guaranteed that the test will always fail if there is a problem.
    // Increasing the number of iterations, also increases the chances of hitting the leak but now 30 is sufficient.
    for (int i = 0; i < 30; i++) {
      String tableName = "table_" + i;
      try {
        ObjectStore s = create();
        s.createTable(newTable(tableName, s.getConf()));
      } catch (Exception e) {
        // The presence of the faulty driver may trigger various exceptions, but we are only interested in those that
        // can cause a connection leak; the rest can be ignored. If the pool cannot provide a connection, it means that
        // there is a leak somewhere (in this case in datanucleus).
        String msg = e.getMessage();
        if (msg != null && msg.startsWith("objectstore-secondary - Connection is not available")) {
          Assertions.fail("Connection leak during creation of " + tableName, e);
        }
      }
    }
  }

  private static Table newTable(String name, Configuration conf) throws MetaException {
    TableBuilder builder = new TableBuilder()
        .setCatName("hive")
        .setDbName("default")
        .setTableName(name)
        .setLocation("file:///path/to/default/" + name);
    builder.addCol("colX", "int");
    return builder.build(conf);
  }

}
