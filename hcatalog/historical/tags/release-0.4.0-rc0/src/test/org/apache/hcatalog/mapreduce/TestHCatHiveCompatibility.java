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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.MiniCluster;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.pig.HCatLoader;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;


public class TestHCatHiveCompatibility extends TestCase {

  MiniCluster cluster = MiniCluster.buildCluster();
  private Driver driver;
  Properties props;

  private HiveMetaStoreClient client;

  String fileName = "/tmp/input.data";
  String fullFileName;

  @Override
  protected void setUp() throws Exception {

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    driver = new Driver(hiveConf);
    client = new HiveMetaStoreClient(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    props = new Properties();
    props.setProperty("fs.default.name", cluster.getProperties().getProperty("fs.default.name"));
    fullFileName = cluster.getProperties().getProperty("fs.default.name") + fileName;

    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];
    for(int i = 0; i < LOOP_SIZE; i++) {
        input[i] = i + "\t1";
    }
    MiniCluster.createInputFile(cluster, fileName, input);
  }

  @Override
  protected void tearDown() throws Exception {
    MiniCluster.deleteFile(cluster, fileName);
  }

  public void testUnpartedReadWrite() throws Exception{

    driver.run("drop table junit_unparted_noisd");
    String createTable = "create table junit_unparted_noisd(a int) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    // assert that the table created has no hcat instrumentation, and that we're still able to read it.
    Table table = client.getTable("default", "junit_unparted_noisd");
    assertTrue(table.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    PigServer server = new PigServer(ExecType.LOCAL, props);
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery("A = load '"+fullFileName+"' as (a:int);");
    server.registerQuery("store A into 'default.junit_unparted_noisd' using org.apache.hcatalog.pig.HCatStorer();");
    server.registerQuery("B = load 'default.junit_unparted_noisd' using "+HCatLoader.class.getName()+"();");
    Iterator<Tuple> itr= server.openIterator("B");

    int i = 0;

    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(1, t.size());
      assertEquals(t.get(0), i);
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(11, i);

    // assert that the table created still has no hcat instrumentation
    Table table2 = client.getTable("default", "junit_unparted_noisd");
    assertTrue(table2.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    driver.run("drop table junit_unparted_noisd");
  }

  public void testPartedRead() throws Exception{

    driver.run("drop table junit_parted_noisd");
    String createTable = "create table junit_parted_noisd(a int) partitioned by (b string) stored as RCFILE";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    // assert that the table created has no hcat instrumentation, and that we're still able to read it.
    Table table = client.getTable("default", "junit_parted_noisd");

    assertTrue(table.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    PigServer server = new PigServer(ExecType.LOCAL, props);
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery("A = load '"+fullFileName+"' as (a:int);");
    server.registerQuery("store A into 'default.junit_parted_noisd' using org.apache.hcatalog.pig.HCatStorer('b=42');");
    server.registerQuery("B = load 'default.junit_parted_noisd' using "+HCatLoader.class.getName()+"();");
    Iterator<Tuple> itr= server.openIterator("B");

    int i = 0;

    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertEquals(t.get(0), i);
      assertEquals(t.get(1), "42");
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(11, i);

    // assert that the table created still has no hcat instrumentation
    Table table2 = client.getTable("default", "junit_parted_noisd");
    assertTrue(table2.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));

    // assert that there is one partition present, and it had hcat instrumentation inserted when it was created.
    Partition ptn = client.getPartition("default", "junit_parted_noisd", Arrays.asList("42"));

    assertNotNull(ptn);
    assertTrue(ptn.getSd().getInputFormat().equals(HCatConstants.HIVE_RCFILE_IF_CLASS));
    driver.run("drop table junit_unparted_noisd");
  }


}
