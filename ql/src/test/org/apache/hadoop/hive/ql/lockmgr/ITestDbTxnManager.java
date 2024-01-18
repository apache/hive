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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Derby;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mariadb;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mssql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Oracle;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Postgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to run DbTxnManager tests against different dbms types.
 * Example: mvn test -Dtest=ITestDbTxnManager -Dtest.metastore.db=postgres
 */
public class ITestDbTxnManager extends TestDbTxnManager2 {

  private static final String SYS_PROP_METASTORE_DB = "test.metastore.db";
  private static final Logger LOG = LoggerFactory.getLogger(TestDbTxnManager2.class);
  private static DatabaseRule rule;


  @BeforeClass
  public static void setupDb() throws Exception {
    String metastoreType =
        System.getProperty(SYS_PROP_METASTORE_DB) == null ? "derby" : System.getProperty(SYS_PROP_METASTORE_DB)
            .toLowerCase();
    rule = getDatabaseRule(metastoreType).setVerbose(false);

    conf.setVar(HiveConf.ConfVars.METASTORE_DB_TYPE, metastoreType.toUpperCase());

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, rule.getJdbcUrl());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, rule.getJdbcDriver());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, rule.getHiveUser());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, rule.getHivePassword());
    // In this case we disable auto_create which is enabled by default for every test
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, false);
    DatabaseProduct.reset();

    LOG.info("Set metastore connection to url: {}",
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY));
    // Start the docker container and create the hive user
    rule.before();
    rule.install();
  }

  @AfterClass
  public static void tearDownDb() {
    rule.after();
  }

  private static DatabaseRule getDatabaseRule(String metastoreType) {
    switch (metastoreType) {
      case "postgres":
        return new Postgres();
      case "oracle":
        return new Oracle();
      case "mysql":
        return new Mysql();
      case "mariadb":
        return new Mariadb();
      case "mssql":
        return new Mssql();
      default:
        return new Derby();
    }
  }
}
