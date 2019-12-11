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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Derby;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mssql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Oracle;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Postgres;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * QTestMetaStoreHandler is responsible for wrapping the logic of handling different metastore
 * databases in qtests.
 */
public class QTestMetaStoreHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestMetaStoreHandler.class);

  private String metastoreType;
  private DatabaseRule rule;

  public QTestMetaStoreHandler() {
    this.metastoreType = QTestSystemProperties.getMetaStoreDb() == null ? "derby"
      : QTestSystemProperties.getMetaStoreDb();

    this.rule = getDatabaseRule(metastoreType).setVerbose(false);

    LOG.info(String.format("initialized metastore type '%s' for qtests", metastoreType));
  }

  public DatabaseRule getRule() {
    return rule;
  }

  public boolean isDerby() {
    return "derby".equalsIgnoreCase(metastoreType);
  }

  public QTestMetaStoreHandler setMetaStoreConfiguration(HiveConf conf) {
    conf.setVar(ConfVars.METASTOREDBTYPE, getDbTypeConfString());

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, rule.getJdbcUrl());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, rule.getJdbcDriver());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, rule.getHiveUser());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, rule.getHivePassword());

    LOG.info(String.format("set metastore connection to url: %s",
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY)));

    return this;
  }

  private DatabaseRule getDatabaseRule(String metastoreType) {
    switch (metastoreType) {
    case "postgres":
      return new Postgres();
    case "oracle":
      return new Oracle();
    case "mysql":
      return new Mysql();
    case "mssql":
    case "sqlserver":
      return new Mssql();
    default:
      return new Derby();
    }
  }

  private String getDbTypeConfString() {// "ORACLE", "MYSQL", "MSSQL", "POSTGRES"
    return "sqlserver".equalsIgnoreCase(metastoreType) ? "MSSQL" : metastoreType.toUpperCase();
  }

  public void beforeTest() throws Exception {
    getRule().before();
    if (!isDerby()) {// derby is handled with old QTestUtil logic (TxnDbUtil stuff)
      getRule().install();
    }
  }

  public void afterTest(QTestUtil qt) throws Exception {
    getRule().after();

    // special qtest logic, which doesn't fit quite well into Derby.after()
    if (isDerby()) {
      TxnDbUtil.cleanDb(qt.getConf());
      TxnDbUtil.prepDb(qt.getConf());
    }
  }

  public void setSystemProperties() {
    System.setProperty(MetastoreConf.ConfVars.CONNECT_URL_KEY.getVarname(), rule.getJdbcUrl());
    System.setProperty(MetastoreConf.ConfVars.CONNECTION_DRIVER.getVarname(), rule.getJdbcDriver());
    System.setProperty(MetastoreConf.ConfVars.CONNECTION_USER_NAME.getVarname(), rule.getHiveUser());
    System.setProperty(MetastoreConf.ConfVars.PWD.getVarname(), rule.getHivePassword());
  }
}
