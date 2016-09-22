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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.jdo.JDOCanRetryException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestObjectStoreInitRetry {

  private static boolean noisy = true; // switch to true to see line number debug traces for FakeDerby calls

  private static int injectConnectFailure = 0;

  public static void setInjectConnectFailure(int x){
    injectConnectFailure = x;
  }

  public static int getInjectConnectFailure(){
    return injectConnectFailure;
  }

  public static void decrementInjectConnectFailure(){
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

  public static void misbehave() throws RuntimeException{
    TestObjectStoreInitRetry.debugTrace();
    if (TestObjectStoreInitRetry.getInjectConnectFailure() > 0){
      TestObjectStoreInitRetry.decrementInjectConnectFailure();
      RuntimeException re = new JDOCanRetryException();
      if (noisy){
        System.err.println("MISBEHAVE:" + TestObjectStoreInitRetry.getInjectConnectFailure());
        re.printStackTrace(System.err);
      }
      throw re;
    }
  }

  // debug instrumenter - useful in finding which fns get called, and how often
  public static void debugTrace() {
    if (noisy){
      Exception e = new Exception();
      System.err.println("." + e.getStackTrace()[1].getLineNumber() + ":" + TestObjectStoreInitRetry.getInjectConnectFailure());
    }
  }

  protected static HiveConf hiveConf;

  @Test
  public void testObjStoreRetry() throws Exception {
    hiveConf = new HiveConf(this.getClass());

    hiveConf.setIntVar(ConfVars.HMSHANDLERATTEMPTS, 4);
    hiveConf.setVar(ConfVars.HMSHANDLERINTERVAL, "1s");
    hiveConf.setVar(ConfVars.METASTORE_CONNECTION_DRIVER,FakeDerby.class.getName());
    hiveConf.setBoolVar(ConfVars.METASTORE_TRY_DIRECT_SQL,true);
    String jdbcUrl = hiveConf.get(ConfVars.METASTORECONNECTURLKEY.varname);
    jdbcUrl = jdbcUrl.replace("derby","fderby");
    hiveConf.setVar(ConfVars.METASTORECONNECTURLKEY,jdbcUrl);

    ObjectStore objStore = new ObjectStore();

    Exception savE = null;
    try {
      setInjectConnectFailure(5);
      objStore.setConf(hiveConf);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      savE = e;
    }

    /**
     * A note on retries.
     *
     * We've configured a total of 4 attempts.
     * 5 - 4 == 1 connect failure simulation count left after this.
     */

    assertEquals(1, getInjectConnectFailure());
    assertNotNull(savE);

    setInjectConnectFailure(0);
    objStore.setConf(hiveConf);
    assertEquals(0, getInjectConnectFailure());
  }

}
