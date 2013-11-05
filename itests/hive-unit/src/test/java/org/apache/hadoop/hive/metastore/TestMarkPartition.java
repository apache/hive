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

package org.apache.hadoop.hive.metastore;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;

public class TestMarkPartition extends TestCase{

  protected HiveConf hiveConf;
  private Driver driver;

  @Override
  protected void setUp() throws Exception {

    super.setUp();
    System.setProperty("hive.metastore.event.clean.freq", "2");
    System.setProperty("hive.metastore.event.expiry.duration", "5");
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(new CliSessionState(hiveConf));

  }

  public void testMarkingPartitionSet() throws CommandNeedRetryException, MetaException,
  TException, NoSuchObjectException, UnknownDBException, UnknownTableException,
  InvalidPartitionException, UnknownPartitionException, InterruptedException {
    HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf, null);
    driver = new Driver(hiveConf);
    driver.run("drop database if exists hive2215 cascade");
    driver.run("create database hive2215");
    driver.run("use hive2215");
    driver.run("drop table if exists tmptbl");
    driver.run("create table tmptbl (a string) partitioned by (b string)");
    driver.run("alter table tmptbl add partition (b='2011')");
    Map<String,String> kvs = new HashMap<String, String>();
    kvs.put("b", "'2011'");
    msc.markPartitionForEvent("hive2215", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    assert msc.isPartitionMarkedForEvent("hive2215", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    Thread.sleep(10000);
    assert !msc.isPartitionMarkedForEvent("hive2215", "tmptbl", kvs, PartitionEventType.LOAD_DONE);

    kvs.put("b", "'2012'");
    assert !msc.isPartitionMarkedForEvent("hive2215", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
    try{
      msc.markPartitionForEvent("hive2215", "tmptbl2", kvs, PartitionEventType.LOAD_DONE);
      assert false;
    } catch(Exception e){
      assert e instanceof UnknownTableException;
    }
    try{
      msc.isPartitionMarkedForEvent("hive2215", "tmptbl2", kvs, PartitionEventType.LOAD_DONE);
      assert false;
    } catch(Exception e){
      assert e instanceof UnknownTableException;
    }
    kvs.put("a", "'2012'");
    try{
      msc.isPartitionMarkedForEvent("hive2215", "tmptbl", kvs, PartitionEventType.LOAD_DONE);
      assert false;
    } catch(Exception e){
      assert e instanceof InvalidPartitionException;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    driver.run("drop database if exists hive2215 cascade");
    super.tearDown();
  }

}
