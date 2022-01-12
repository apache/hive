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
import org.apache.hadoop.hive.metastore.database.MetastoreDatabaseWrapper;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * QTestMetaStoreHandler is responsible for wrapping the logic of handling different metastore
 * databases in qtests.
 */
public class QTestMetaStoreHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestMetaStoreHandler.class);

  private final String metastoreType;
  private final MetastoreDatabaseWrapper metastoreDb;

  public QTestMetaStoreHandler(String metastore) {
    this.metastoreType = Objects.requireNonNull(metastore);

    this.metastoreDb = MetastoreDatabaseWrapper.getMetastoreDatabase(metastoreType);

    LOG.info(String.format("initialized metastore type '%s' for qtests", metastoreType));
  }

  public MetastoreDatabaseWrapper getMetastoreDb() {
    return metastoreDb;
  }

  public boolean isDerby() {
    return "derby".equalsIgnoreCase(metastoreType);
  }

  public QTestMetaStoreHandler setMetaStoreConfiguration(HiveConf conf) {
    conf.setVar(ConfVars.METASTOREDBTYPE, getDbTypeConfString());

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, metastoreDb.getDatabase().getJdbcUrl());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, metastoreDb.getDatabase().getJdbcDriver());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, metastoreDb.getDatabase().getHiveUser());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, metastoreDb.getDatabase().getHivePassword());
    // In this case we can disable auto_create which is enabled by default for every test
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, false);

    LOG.info(String.format("set metastore connection to url: %s",
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY)));

    return this;
  }

  private String getDbTypeConfString() {// "ORACLE", "MYSQL", "MSSQL", "POSTGRES"
    return "sqlserver".equalsIgnoreCase(metastoreType) ? "MSSQL" : metastoreType.toUpperCase();
  }

  public void beforeTest() throws Exception {
    metastoreDb.before();
    if (!isDerby()) {// derby is handled with old QTestUtil logic (TxnDbUtil stuff)
      metastoreDb.install();
    }
  }

  public void afterTest(QTestUtil qt) throws Exception {
    metastoreDb.after();

    // special qtest logic, which doesn't fit quite well into Derby.after()
    if (isDerby()) {
      TestTxnDbUtil.cleanDb(qt.getConf());
    }
  }

  public void setSystemProperties() {
    System.setProperty(MetastoreConf.ConfVars.CONNECT_URL_KEY.getVarname(), metastoreDb.getDatabase().getJdbcUrl());
    System.setProperty(MetastoreConf.ConfVars.CONNECTION_DRIVER.getVarname(), metastoreDb.getDatabase().getJdbcDriver());
    System.setProperty(MetastoreConf.ConfVars.CONNECTION_USER_NAME.getVarname(), metastoreDb.getDatabase().getHiveUser());
    System.setProperty(MetastoreConf.ConfVars.PWD.getVarname(), metastoreDb.getDatabase().getHivePassword());
    System.setProperty(MetastoreConf.ConfVars.AUTO_CREATE_ALL.getVarname(), "false");
  }
}
