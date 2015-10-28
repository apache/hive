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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests with HBase Mini-cluster using actual SQL
 */
public class TestHBaseMetastoreSql extends HBaseIntegrationTests {

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStoreIntegration.class.getName());

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();

  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    setupConnection();
    setupDriver();
  }

  @Test
  public void insertIntoTable() throws Exception {
    driver.run("create table iit (c int)");
    CommandProcessorResponse rsp = driver.run("insert into table iit values (3)");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void insertIntoPartitionTable() throws Exception {
    driver.run("create table iipt (c int) partitioned by (ds string)");
    CommandProcessorResponse rsp =
        driver.run("insert into table iipt partition(ds) values (1, 'today'), (2, 'yesterday')," +
            "(3, 'tomorrow')");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void database() throws Exception {
    CommandProcessorResponse rsp = driver.run("create database db");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("set role admin");
    Assert.assertEquals(0, rsp.getResponseCode());
    // security doesn't let me change the properties
    rsp = driver.run("alter database db set dbproperties ('key' = 'value')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("drop database db");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void table() throws Exception {
    driver.run("create table tbl (c int)");
    CommandProcessorResponse rsp = driver.run("insert into table tbl values (3)");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("select * from tbl");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table tbl set tblproperties ('example', 'true')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("drop table tbl");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void partitionedTable() throws Exception {
    driver.run("create table parttbl (c int) partitioned by (ds string)");
    CommandProcessorResponse rsp =
        driver.run("insert into table parttbl partition(ds) values (1, 'today'), (2, 'yesterday')" +
            ", (3, 'tomorrow')");
    Assert.assertEquals(0, rsp.getResponseCode());
    // Do it again, to check insert into existing partitions
    rsp = driver.run("insert into table parttbl partition(ds) values (4, 'today'), (5, 'yesterday')"
        + ", (6, 'tomorrow')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("insert into table parttbl partition(ds = 'someday') values (1)");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("insert into table parttbl partition(ds = 'someday') values (2)");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table parttbl add partition (ds = 'whenever')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("insert into table parttbl partition(ds = 'whenever') values (2)");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table parttbl touch partition (ds = 'whenever')");
    Assert.assertEquals(0, rsp.getResponseCode());
    // TODO - Can't do this until getPartitionsByExpr implemented
    /*
    rsp = driver.run("alter table parttbl drop partition (ds = 'whenever')");
    Assert.assertEquals(0, rsp.getResponseCode());
    */
    rsp = driver.run("select * from parttbl");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("select * from parttbl where ds = 'today'");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void role() throws Exception {
    CommandProcessorResponse rsp = driver.run("set role admin");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("create role role1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("grant role1 to user fred with admin option");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("create role role2");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("grant role1 to role role2");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("show principals role1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("show role grant role role1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("show role grant user " + System.getProperty("user.name"));
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("show roles");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("revoke admin option for role1 from user fred");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("revoke role1 from user fred");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("revoke role1 from role role2");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("show current roles");
    Assert.assertEquals(0, rsp.getResponseCode());

    rsp = driver.run("drop role role2");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("drop role role1");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void grant() throws Exception {
    CommandProcessorResponse rsp = driver.run("set role admin");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("create role role3");
    Assert.assertEquals(0, rsp.getResponseCode());
    driver.run("create table granttbl (c int)");
    Assert.assertEquals(0, rsp.getResponseCode());
    driver.run("grant select on granttbl to " + System.getProperty("user.name"));
    Assert.assertEquals(0, rsp.getResponseCode());
    driver.run("grant select on granttbl to role3 with grant option");
    Assert.assertEquals(0, rsp.getResponseCode());
    driver.run("revoke select on granttbl from " + System.getProperty("user.name"));
    Assert.assertEquals(0, rsp.getResponseCode());
    driver.run("revoke grant option for select on granttbl from role3");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void describeNonpartitionedTable() throws Exception {
    CommandProcessorResponse rsp = driver.run("create table alter1(a int, b int)");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe extended alter1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table alter1 set serdeproperties('s1'='9')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe extended alter1");
    Assert.assertEquals(0, rsp.getResponseCode());
  }

  @Test
  public void alterRenamePartitioned() throws Exception {
    driver.run("create table alterrename (c int) partitioned by (ds string)");
    driver.run("alter table alterrename add partition (ds = 'a')");
    CommandProcessorResponse rsp = driver.run("describe extended alterrename partition (ds='a')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table alterrename rename to alter_renamed");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe extended alter_renamed partition (ds='a')");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe extended alterrename partition (ds='a')");
    Assert.assertEquals(10001, rsp.getResponseCode());
  }

  @Test
  public void alterRename() throws Exception {
    driver.run("create table alterrename1 (c int)");
    CommandProcessorResponse rsp = driver.run("describe alterrename1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("alter table alterrename1 rename to alter_renamed1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe alter_renamed1");
    Assert.assertEquals(0, rsp.getResponseCode());
    rsp = driver.run("describe alterrename1");
    Assert.assertEquals(10001, rsp.getResponseCode());
  }


}
