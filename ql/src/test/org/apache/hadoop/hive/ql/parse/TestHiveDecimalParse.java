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

package org.apache.hadoop.hive.ql.parse;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

public class TestHiveDecimalParse {

  @Test
  public void testDecimalType() throws ParseException {
    String query = "create table dec (d decimal)";
    String type = getColumnType(query);
    Assert.assertEquals("decimal(10,0)", type);
  }

  @Test
  public void testDecimalType1() throws ParseException {
    String query = "create table dec (d decimal(5))";
    String type = getColumnType(query);
    Assert.assertEquals("decimal(5,0)", type);
  }

  @Test
  public void testDecimalType2() throws ParseException {
    String query = "create table dec (d decimal(9,7))";
    String type = getColumnType(query);
    Assert.assertEquals("decimal(9,7)", type);
  }

  @Test
  public void testDecimalType3() throws ParseException {
    String query = "create table dec (d decimal(66,7))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("Decimal precision out of allowed range [1,38]"));
  }

  @Test
  public void testDecimalType4() throws ParseException {
    String query = "create table dec (d decimal(0,7))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("Decimal precision out of allowed range [1,38]"));
  }

  @Test
  public void testDecimalType5() throws ParseException {
    String query = "create table dec (d decimal(7,33))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("Decimal scale must be less than or equal to precision"));
  }

  @Test
  public void testDecimalType6() throws ParseException {
    String query = "create table dec (d decimal(7,-1))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("extraneous input '-' expecting Number"));
  }

  @Test
  public void testDecimalType7() throws ParseException {
    String query = "create table dec (d decimal(7,33,4))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
      driver.getErrorMsg().contains("missing ) at ',' near ',' in column name or primary key or foreign key"));
  }

  @Test
  public void testDecimalType8() throws ParseException {
    String query = "create table dec (d decimal(7a))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("mismatched input '7a' expecting Number near '('"));
  }

  @Test
  public void testDecimalType9() throws ParseException {
    String query = "create table dec (d decimal(20,23))";

    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertTrue("Got " + rc + ", expected not zero", rc != 0);
    Assert.assertTrue(driver.getErrorMsg(),
        driver.getErrorMsg().contains("Decimal scale must be less than or equal to precision"));
  }

  private Driver createDriver() {
    HiveConf conf = new HiveConf(Driver.class);
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

  private String getColumnType(String query) {
    Driver driver = createDriver();
    int rc = driver.compile(query);

    if (rc != 0) {
      return null;
    }

    QueryPlan plan = driver.getPlan();
    DDLTask task = (DDLTask) plan.getRootTasks().get(0);
    DDLWork work = task.getWork();
    CreateTableDesc spec = work.getCreateTblDesc();
    FieldSchema fs = spec.getCols().get(0);
    return fs.getType();
  }

}
