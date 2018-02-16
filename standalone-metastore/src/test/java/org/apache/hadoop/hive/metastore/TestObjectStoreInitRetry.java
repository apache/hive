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

package org.apache.hadoop.hive.metastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.jdo.JDOCanRetryException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(MetastoreCheckinTest.class)
public class TestObjectStoreInitRetry {
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectStoreInitRetry.class);

  private static int injectConnectFailure = 0;

  private static void setInjectConnectFailure(int x){
    injectConnectFailure = x;
  }

  private static int getInjectConnectFailure(){
    return injectConnectFailure;
  }

  private static void decrementInjectConnectFailure(){
    injectConnectFailure--;
  }

  @BeforeClass
  public static void oneTimeSetup() throws SQLException {
    // dummy instantiation to make sure any static/ctor code blocks of that
    // driver are loaded and ready to go.
    DriverManager.registerDriver(new FakeDerby());
  }

  @AfterClass
  public static void oneTimeTearDown() throws SQLException {
    DriverManager.deregisterDriver(new FakeDerby());
  }

  static void misbehave() throws RuntimeException{
    TestObjectStoreInitRetry.debugTrace();
    if (TestObjectStoreInitRetry.getInjectConnectFailure() > 0){
      TestObjectStoreInitRetry.decrementInjectConnectFailure();
      RuntimeException re = new JDOCanRetryException();
      LOG.debug("MISBEHAVE:" + TestObjectStoreInitRetry.getInjectConnectFailure(), re);
      throw re;
    }
  }

  // debug instrumenter - useful in finding which fns get called, and how often
  static void debugTrace() {
    if (LOG.isDebugEnabled()){
      Exception e = new Exception();
      LOG.debug("." + e.getStackTrace()[1].getLineNumber() + ":" + TestObjectStoreInitRetry.getInjectConnectFailure());
    }
  }

  protected static Configuration conf;

  @Test
  public void testObjStoreRetry() throws Exception {
    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setLongVar(conf, ConfVars.HMSHANDLERATTEMPTS, 4);
    MetastoreConf.setTimeVar(conf, ConfVars.HMSHANDLERINTERVAL, 1, TimeUnit.SECONDS);
    MetastoreConf.setVar(conf, ConfVars.CONNECTION_DRIVER,FakeDerby.class.getName());
    MetastoreConf.setBoolVar(conf, ConfVars.TRY_DIRECT_SQL,true);
    String jdbcUrl = MetastoreConf.getVar(conf, ConfVars.CONNECTURLKEY);
    jdbcUrl = jdbcUrl.replace("derby","fderby");
    MetastoreConf.setVar(conf, ConfVars.CONNECTURLKEY,jdbcUrl);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    FakeDerby fd = new FakeDerby();

    ObjectStore objStore = new ObjectStore();

    Exception savE = null;
    try {
      setInjectConnectFailure(5);
      objStore.setConf(conf);
      Assert.fail();
    } catch (Exception e) {
      LOG.info("Caught exception ", e);
      savE = e;
    }

    /*
     * A note on retries.
     *
     * We've configured a total of 4 attempts.
     * 5 - 4 == 1 connect failure simulation count left after this.
     */

    assertEquals(1, getInjectConnectFailure());
    assertNotNull(savE);

    setInjectConnectFailure(0);
    objStore.setConf(conf);
    assertEquals(0, getInjectConnectFailure());
  }

}
