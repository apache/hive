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
package org.apache.hadoop.hive.ql.plan.mapping;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestAA {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
    String[] cmds = {
        // @formatter:off
        "create table tu(id_uv int,id_uw int,u int)",
        "CREATE EXTERNAL TABLE table_name(\n" +
        "`test_col_m`       string,\n" +
        "`test_strt_d`       string,\n" +
        "`test_end_d`      string,\n" +
        "`test_col_1`        string,\n" +
        "`test_col_2`        string,\n" +
        "`inpt_colm_1_x`                string,\n" +
        "`inpt_colm_2_x`                string,\n" +
        "`inpt_colm_3_x`                string,\n" +
        "`inpt_colm_4_x`                string,\n" +
        "`inpt_colm_5_x`                string,\n" +
        "`inpt_colm_6_x`                string,\n" +
        "`inpt_colm_7_x`                string,\n" +
        "`inpt_colm_8_x`                string,\n" +
        "`inpt_colm_9_x`                string,\n" +
        "`inpt_colm_10_x`             string,\n" +
        "`oput_colm_1_x`              string,\n" +
        "`oput_colm_2_x`              string,\n" +
        "`oput_colm_3_x`              string,\n" +
        "`oput_colm_4_x`              string,\n" +
        "`oput_colm_5_x`              string,\n" +
        "`oput_colm_6_x`              string,\n" +
        "`oput_colm_7_x`              string,\n" +
        "`oput_colm_8_x`              string,\n" +
        "`oput_colm_9_x`              string,\n" +
        "`oput_colm_10_x`            string,\n" +
        "`oput_colm_11_x`            decimal(15,2),\n" +
        "`oput_colm_12_x`            string,\n" +
        "`oput_colm_13_x`            string,\n" +
        "`oput_colm_14_x`            timestamp,\n" +
        "`oput_colm_15_x`            timestamp,\n" +
        "`oput_colm_16_x`            timestamp,\n" +
        "`year`  string,\n" +
        "`month` string,\n" +
        "`day`   string\n" +
        ")\n" +
        "",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String[] tables = new String[] { "tu", "table_name" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  @Test
  public void testQ1() throws Exception {
    IDriver driver = createDriver();

    String h = "INSERT OVERWRITE TABLE table_name ";

    String b =
        "SELECT 'ABCD EFGH','1900-01-01','9999-12-31','MAS','XYZ RTQU','ABC','KIT TYPE','LOCK TYPE','RATE_PERCENTAGE','RATE_DOLLAR','MXX','MXD','','','','2121','42','1392','0','NOT APPLICABLE','NOT APPLICABLE','NOT APPLICABLE','','','','','1900-01-01','9999-12-31',from_unixtime(unix_timestamp(\"1900-01-01 00:45:39\", \"yyyy-MM-dd HH:mm:ss\")),from_unixtime(unix_timestamp(\"9999-12-31 00:59:59\", \"yyyy-MM-ddHH:mm:ss\")),CURRENT_TIMESTAMP(),substr(current_timestamp(),1,4),substr(current_timestamp(),6,2),substr(current_timestamp(),9,2) ";
    String c = "UNION ALL ";

    StringBuilder sb = new StringBuilder();

    sb.append(h);
    sb.append(b);
    for (int i = 0; i < 20; i++) {
      sb.append(c);
      sb.append(b);
    }

    String query = sb.toString();

    driver.run(query);

  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }

}
