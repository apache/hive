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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandlerContext;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestProfilingDataSource {
  private Configuration conf;
  private Counter slowQuery;

  @Before
  public void init() {
    conf = MetastoreConf.newMetastoreConf();
    conf.set(MetastoreStatement.EXEC_HOOK, MetastoreStatementTestHook.class.getName());
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.METASTORE_JDBC_SLOW_QUERY_THRESHOLD, 200, TimeUnit.MILLISECONDS);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_EXECUTION, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PROFILE_JDBC_THRIFT_APIS, "test_metastore_statement");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, "dummyUser");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, "dummyPass");
    conf.unset(MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE.getVarname());

    Metrics.initialize(conf);
    slowQuery = Metrics.getOrCreateCounter(MetricsConstants.JDBC_SLOW_QUERIES);
  }

  @Test
  public void testDefaultHikariCp() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE, HikariCPDataSourceProvider.HIKARI);

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);
    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof ProfilingDataSource);
    Assert.assertTrue(((ProfilingDataSource) ds).getDelegate() instanceof HikariDataSource);
    try (Connection connection = ds.getConnection()) {
      verify(connection);
    }
  }

  @Test
  public void testDbCpDataSource() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE, DbCPDataSourceProvider.DBCP);

    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    Assert.assertNotNull(dsp);
    DataSource ds = dsp.create(conf);
    Assert.assertTrue(ds instanceof ProfilingDataSource);
    Assert.assertTrue(((ProfilingDataSource) ds).getDelegate() instanceof PoolingDataSource);
    try (Connection connection = ds.getConnection()) {
      verify(connection);
    }
  }

  private void verify(Connection connection) throws Exception {
    // Assert.assertTrue(connection.unwrap(MetastoreConnection.class).delegate() instanceof EmbedConnection);
    long slowNum = slowQuery.getCount();
    Timer timer = Metrics.getOrCreateTimer(MetastoreStatementTestHook.TEST_METRIC_NAME);
    Assert.assertNotNull(timer);
    long timeCount = timer.getCount();
    try (AutoCloseable sleep = MetastoreStatementTestHook.testConnection("test_metastore_statement", 300, connection)) {
      try (Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery("VALUES 1")) {
        Assert.assertTrue(rs.next());
      }
      HMSHandlerContext.getCallCtx().ifPresent(ctx -> {
        Assert.assertEquals(1, ctx.getQueryCount());
        Assert.assertTrue(ctx.getTotalTime() >= 300);
        Assert.assertTrue(ctx.getMaxJdbcTimeMs() >= 300);
      });
    }
    Assert.assertEquals(slowNum + 1, slowQuery.getCount());
    Assert.assertEquals(timeCount + 1, timer.getCount());
    Assert.assertTrue(timer.getSnapshot().getMean() > TimeUnit.MILLISECONDS.toNanos(300));

    try (AutoCloseable sleep = MetastoreStatementTestHook.testConnection("test_statement_outside", 300, connection)) {
      try (Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery("VALUES 1")) {
        Assert.assertTrue(rs.next());
      }
    }
    Assert.assertEquals(slowNum + 2, slowQuery.getCount());
    Assert.assertEquals(timeCount + 1, timer.getCount());
  }

  public static class MetastoreStatementTestHook extends MetastoreStatement.ThriftApiProfiler {
    static final String TEST_METRIC_NAME = "MetastoreStatementTestHook_" + System.currentTimeMillis();
    static final String SLEEP_MILLIS = "MetastoreStatementTestHook.sleep.ms";
    private final Configuration configuration;

    public MetastoreStatementTestHook(Configuration configuration) {
      super(configuration);
      this.configuration = configuration;
    }

    @Override
    public void preRun(Method method, Object[] args) {
      long sleepMs = configuration.getLong(SLEEP_MILLIS, 0);
      if (sleepMs > 0 &&
          MetastoreStatement.QUERY_EXECUTION.contains(method.getName())) {
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public String getMetricName(Method method) {
      return TEST_METRIC_NAME;
    }

    public static AutoCloseable testConnection(String method, long sleepMs, Connection connection)
        throws Exception {
      HMSHandlerContext.setCallCtx(new HMSHandlerContext.CallCtx(method));
      Configuration configuration = connection.unwrap(MetastoreConnection.class).getConfiguration();
      configuration.set(SLEEP_MILLIS, sleepMs + "");
      return () -> {
        HMSHandlerContext.setCallCtx(null);
        configuration.unset(SLEEP_MILLIS);
      };
    }
  }
}
