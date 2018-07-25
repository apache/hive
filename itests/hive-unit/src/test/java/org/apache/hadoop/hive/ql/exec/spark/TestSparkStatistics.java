/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import com.google.common.collect.Lists;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistic;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsNames;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestSparkStatistics {

  @Ignore("flaky test")
  @Test
  public void testSparkStatistics() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            SQLStdHiveAuthorizerFactory.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "spark");
    conf.set("spark.master", "local-cluster[1,2,1024]");

    SessionState.start(conf);

    Driver driver = null;

    try {
      driver = new Driver(new QueryState.Builder()
              .withGenerateNewQueryId(true)
              .withHiveConf(conf).build(),
              null, null);

      Assert.assertEquals(0, driver.run("create table test (col int)").getResponseCode());
      Assert.assertEquals(0, driver.compile("select * from test order by col"));

      List<SparkTask> sparkTasks = Utilities.getSparkTasks(driver.getPlan().getRootTasks());
      Assert.assertEquals(1, sparkTasks.size());

      SparkTask sparkTask = sparkTasks.get(0);

      DriverContext driverCxt = new DriverContext(driver.getContext());
      driverCxt.prepare(driver.getPlan());

      sparkTask.initialize(driver.getQueryState(), driver.getPlan(), driverCxt, driver.getContext()
              .getOpContext());
      Assert.assertEquals(0, sparkTask.execute(driverCxt));

      Assert.assertNotNull(sparkTask.getSparkStatistics());

      List<SparkStatistic> sparkStats = Lists.newArrayList(sparkTask.getSparkStatistics()
              .getStatisticGroup(SparkStatisticsNames.SPARK_GROUP_NAME).getStatistics());

      Assert.assertEquals(18, sparkStats.size());

      Map<String, String> statsMap = sparkStats.stream().collect(
              Collectors.toMap(SparkStatistic::getName, SparkStatistic::getValue));

      Assert.assertTrue(Long.parseLong(statsMap.get(SparkStatisticsNames.TASK_DURATION_TIME)) > 0);
      Assert.assertTrue(Long.parseLong(statsMap.get(SparkStatisticsNames.EXECUTOR_CPU_TIME)) > 0);
      Assert.assertTrue(
              Long.parseLong(statsMap.get(SparkStatisticsNames.EXECUTOR_DESERIALIZE_CPU_TIME)) > 0);
      Assert.assertTrue(
              Long.parseLong(statsMap.get(SparkStatisticsNames.EXECUTOR_DESERIALIZE_TIME)) > 0);
      Assert.assertTrue(Long.parseLong(statsMap.get(SparkStatisticsNames.EXECUTOR_RUN_TIME)) > 0);
    } finally {
      if (driver != null) {
        Assert.assertEquals(0, driver.run("drop table if exists test").getResponseCode());
        driver.destroy();
      }
    }
  }
}
