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
package org.apache.hadoop.hive.metastore.datasource;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.jdo.PersistenceManagerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Category(MetastoreUnitTest.class)
public class TestDataSourceProviderFactory {

  private Configuration conf;

  @Before
  public void init() {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.CONNECTION_USER_NAME, "dummyUser");
    MetastoreConf.setVar(conf, ConfVars.PWD, "dummyPass");
    conf.unset(ConfVars.CONNECTION_POOLING_TYPE.getVarname());
  }

  @Test
  public void testNoDataSourceCreatedWithoutProps() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, "dummy");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNull(dsp);
  }

  @Test
  public void testSetHikariCpLeakDetectionThresholdProperty() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".leakDetectionThreshold", "3600");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals(3600L, ((HikariDataSource)ds).getLeakDetectionThreshold());
  }

  @Test
  public void testCreateHikariCpDataSource() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    // This is needed to prevent the HikariDataSource from trying to connect to the DB
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
  }

  @Test
  public void testSetHikariCpStringProperty() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".connectionInitSql", "select 1 from dual");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals("select 1 from dual", ((HikariDataSource)ds).getConnectionInitSql());
  }

  @Test
  public void testSetHikariCpNumberProperty() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".idleTimeout", "59999");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals(59999L, ((HikariDataSource)ds).getIdleTimeout());
  }

  @Test
  public void testSetHikariCpBooleanProperty() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);
    conf.set(HikariCPDataSourceProvider.HIKARI + ".allowPoolSuspension", "false");
    conf.set(HikariCPDataSourceProvider.HIKARI + ".initializationFailTimeout", "-1");

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof HikariDataSource);
    Assert.assertEquals(false, ((HikariDataSource)ds).isAllowPoolSuspension());
  }

  @Test
  public void testCreateDbCpDataSource() throws SQLException {

    MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, DbCPDataSourceProvider.DBCP);

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof PoolingDataSource);
  }

  @Test
  public void testClosePersistenceManagerProvider() throws Exception {
    String[] dataSourceType = {HikariCPDataSourceProvider.HIKARI, DbCPDataSourceProvider.DBCP};
    for (String type : dataSourceType) {
      boolean isHikari = HikariCPDataSourceProvider.HIKARI.equals(type);
      MetastoreConf.setVar(conf, ConfVars.CONNECTION_POOLING_TYPE, type);
      PersistenceManagerProvider.updatePmfProperties(conf);
      PersistenceManagerFactory factory =
          PersistenceManagerProvider.getPersistenceManager().getPersistenceManagerFactory();
      DataSource connFactory = (DataSource) factory.getConnectionFactory();
      DataSource connFactory2 = (DataSource) factory.getConnectionFactory2();
      factory.close();
      // Closing PersistenceManagerFactory does not shut down the connection factory
      // For DBCP, connFactory.getConnection() will return a connection successfully when the pool is not shutdown
      if (isHikari) {
        Assert.assertFalse(((HikariDataSource)connFactory).isClosed());
        Assert.assertFalse(((HikariDataSource)connFactory2).isClosed());
      }
      // Underlying connection is still able to run query
      try (Connection conn = connFactory.getConnection()) {
        Assert.assertFalse(conn.isClosed());
      }
      // Close the underlying connection pools
      PersistenceManagerProvider.closePmfInternal(factory);
      if (isHikari) {
        Assert.assertTrue(((HikariDataSource)connFactory).isClosed());
        Assert.assertTrue(((HikariDataSource)connFactory2).isClosed());
      }
      try {
        connFactory.getConnection();
        Assert.fail("Should fail as the DataSource is shutdown");
      } catch (Exception e) {
        if (isHikari) {
          Assert.assertTrue(e instanceof SQLException);
        } else {
          Assert.assertTrue(e instanceof IllegalStateException);
        }
      }
    }
  }
}
