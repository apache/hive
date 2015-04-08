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

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class TestMetastoreVersion extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestMetastoreVersion.class);
  protected HiveConf hiveConf;
  private Driver driver;
  private String metaStoreRoot;
  private String testMetastoreDB;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Field defDb = HiveMetaStore.HMSHandler.class.getDeclaredField("currentUrl");
    defDb.setAccessible(true);
    defDb.set(null, null);
    // reset defaults
    ObjectStore.setSchemaVerified(false);
    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");
    System.setProperty(HiveConf.ConfVars.METASTORE_AUTO_CREATE_SCHEMA.toString(), "true");
    System.setProperty(HiveConf.ConfVars.METASTORE_FIXED_DATASTORE.toString(), "false");
    hiveConf = new HiveConf(this.getClass());
    System.setProperty("hive.support.concurrency", "false");
    System.setProperty("hive.metastore.event.listeners",
        DummyListener.class.getName());
    System.setProperty("hive.metastore.pre.event.listeners",
        DummyPreListener.class.getName());
    testMetastoreDB = System.getProperty("java.io.tmpdir") +
      File.separator + "test_metastore-" + System.currentTimeMillis();
    System.setProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
        "jdbc:derby:" + testMetastoreDB + ";create=true");
    metaStoreRoot = System.getProperty("test.tmp.dir");
  }

  @Override
  protected void tearDown() throws Exception {
    File metaStoreDir = new File(testMetastoreDB);
    if (metaStoreDir.exists()) {
      FileUtils.forceDeleteOnExit(metaStoreDir);
    }
  }

  /***
   * Test config defaults
   */
  public void testDefaults() {
    System.clearProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString());
    hiveConf = new HiveConf(this.getClass());
    assertFalse(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION));
    assertTrue(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_SCHEMA));
    assertFalse(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FIXED_DATASTORE));
  }

  /***
   * Test schema verification property
   * @throws Exception
   */
  public void testVersionRestriction () throws Exception {
    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "true");
    hiveConf = new HiveConf(this.getClass());
    assertTrue(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION));
    assertFalse(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_SCHEMA));
    assertTrue(hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_FIXED_DATASTORE));

    // session creation should fail since the schema didn't get created
    try {
      SessionState.start(new CliSessionState(hiveConf));
      fail("Expected exception");
    } catch (RuntimeException re) {
      LOG.info("Exception in testVersionRestriction: " + re, re);
      String msg = HiveStringUtils.stringifyException(re);
      assertTrue("Expected 'Version information not found in metastore' in: " + msg, msg
        .contains("Version information not found in metastore"));
    }
  }

  /***
   * Test that with no verification, hive populates the schema and version correctly
   * @throws Exception
   */
  public void testMetastoreVersion () throws Exception {
    // let the schema and version be auto created
    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");
    hiveConf = new HiveConf(this.getClass());
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
    driver.run("show tables");

    // correct version stored by Metastore during startup
    assertEquals(MetaStoreSchemaInfo.getHiveSchemaVersion(), getVersion(hiveConf));
    setVersion(hiveConf, "foo");
    assertEquals("foo", getVersion(hiveConf));
  }

  /***
   * Test that with verification enabled, hive works when the correct schema is already populated
   * @throws Exception
   */
  public void testVersionMatching () throws Exception {
    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");
    hiveConf = new HiveConf(this.getClass());
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
    driver.run("show tables");

    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, true);
    setVersion(hiveConf, MetaStoreSchemaInfo.getHiveSchemaVersion());
    driver = new Driver(hiveConf);
    CommandProcessorResponse proc = driver.run("show tables");
    assertTrue(proc.getResponseCode() == 0);
  }

  /**
   * Store garbage version in metastore and verify that hive fails when verification is on
   * @throws Exception
   */
  public void testVersionMisMatch () throws Exception {
    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");
    hiveConf = new HiveConf(this.getClass());
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
    driver.run("show tables");

    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "true");
    hiveConf = new HiveConf(this.getClass());
    setVersion(hiveConf, "fooVersion");
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
    CommandProcessorResponse proc = driver.run("show tables");
    assertEquals(0, proc.getResponseCode());
  }

  //  write the given version to metastore
  private String getVersion(HiveConf conf) throws HiveMetaException {
    MetaStoreSchemaInfo schemInfo = new MetaStoreSchemaInfo(metaStoreRoot, conf, "derby");
    return getMetaStoreVersion();
  }

  //  write the given version to metastore
  private void setVersion(HiveConf conf, String version) throws HiveMetaException {
    MetaStoreSchemaInfo schemInfo = new MetaStoreSchemaInfo(metaStoreRoot, conf, "derby");
    setMetaStoreVersion(version, "setVersion test");
  }

  // Load the version stored in the metastore db
  public String getMetaStoreVersion() throws HiveMetaException {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(hiveConf);
    try {
      return objStore.getMetaStoreSchemaVersion();
    } catch (MetaException e) {
      throw new HiveMetaException("Failed to get version", e);
    }
  }

  // Store the given version and comment in the metastore
  public void setMetaStoreVersion(String newVersion, String comment) throws HiveMetaException {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(hiveConf);
    try {
      objStore.setMetaStoreSchemaVersion(newVersion, comment);
    } catch (MetaException e) {
      throw new HiveMetaException("Failed to set version", e);
    }
  }


}

