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
package org.apache.hadoop.hive.metastore.datasource;

import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.SQLException;

public class TestDataSourceProviderFactory {

  private HiveConf conf;

  @Before
  public void init() {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, "dummyUser");
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, "dummyPass");
  }

  @Test
  public void testNoDataSourceCreatedWithoutProps() throws SQLException {

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNull(dsp);

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);

    dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNull(dsp);
  }

  @Test
  public void testCreateBoneCpDataSource() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".firstProp", "value");
    conf.set(BoneCPDataSourceProvider.BONECP + ".secondProp", "value");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
  }

  @Test
  public void testSetBoneCpStringProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".initSQL", "select 1 from dual");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals("select 1 from dual", ((BoneCPDataSource)ds).getInitSQL());
  }

  @Test
  public void testSetBoneCpNumberProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".acquireRetryDelayInMs", "599");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals(599L, ((BoneCPDataSource)ds).getAcquireRetryDelayInMs());
  }

  @Test
  public void testSetBoneCpBooleanProperty() throws SQLException {

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, BoneCPDataSourceProvider.BONECP);
    conf.set(BoneCPDataSourceProvider.BONECP + ".disableJMX", "true");

    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    Assert.assertNotNull(dsp);

    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof BoneCPDataSource);
    Assert.assertEquals(true, ((BoneCPDataSource)ds).isDisableJMX());
  }
}
