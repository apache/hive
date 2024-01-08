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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.server.HiveServer2;
import org.junit.Before;
import org.junit.Test;

public class TestServerSpecificConfig {

  private static URL oldDefaultHiveSite = HiveConf.getHiveSiteLocation();

  /**
   * Verify if appropriate server configuration (metastore, hiveserver2) get
   * loaded when the embedded clients are loaded
   *
   * Checks values used in the configs used for testing.
   *
   * @throws IOException
   * @throws Throwable
   */
  @Test
  public void testServerConfigsEmbeddedMetastore() throws IOException, Throwable {

    // set hive-site.xml to default hive-site.xml that has embedded metastore
    HiveConf.setHiveSiteLocation(oldDefaultHiveSite);

    HiveConf conf = new HiveConf();

    // check config properties expected with embedded metastore client
    assertTrue(HiveConf.isLoadMetastoreConfig());
    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));

    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));

    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hivesite"));

    // verify that hiveserver2 config is not loaded
    assertFalse(HiveConf.isLoadHiveServer2Config());
    assertNull(conf.get("hive.dummyparam.test.server.specific.config.hiveserver2site"));

    // check if hiveserver2 config gets loaded when HS2 is started
    new HiveServer2();
    conf = new HiveConf();
    verifyHS2ConfParams(conf);

    assertEquals("from.hivemetastore-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));
  }

  private void verifyHS2ConfParams(HiveConf conf) {
    assertTrue(HiveConf.isLoadHiveServer2Config());
    assertEquals("from.hiveserver2-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));

    assertEquals("from.hiveserver2-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hiveserver2site"));

    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.hivesite"));
  }

  /**
   * Ensure that system properties still get precedence. Config params set as
   * -hiveconf on commandline get set as system properties They should have the
   * final say
   */
  @Test
  public void testSystemPropertyPrecedence() {
    // Using property defined in HiveConf.ConfVars to test System property
    // overriding
    final String OVERRIDE_KEY = "hive.conf.restricted.list";
    try {
      HiveConf.setHiveSiteLocation(oldDefaultHiveSite);
      System.setProperty(OVERRIDE_KEY, "from.sysprop");
      HiveConf conf = new HiveConf();
      // ensure metatore site.xml does not get to override this
      assertEquals("from.sysprop", conf.get(OVERRIDE_KEY));

      // get HS2 site.xml loaded
      new HiveServer2();
      conf = new HiveConf();
      assertTrue(HiveConf.isLoadHiveServer2Config());
      // ensure hiveserver2 site.xml does not get to override this
      assertEquals("from.sysprop", conf.get(OVERRIDE_KEY));

    } finally {
      System.getProperties().remove(OVERRIDE_KEY);
    }
  }

  @Before
  public void resetDefaults() throws SecurityException, IllegalArgumentException,
      NoSuchFieldException, IllegalAccessException {
    // re-set the static variables in HiveConf to default values

    // set load server conf booleans to false
    HiveConf.setLoadMetastoreConfig(false);
    HiveConf.setLoadHiveServer2Config(false);

  }

  /**
   * Test to ensure that HiveConf does not try to load hivemetastore-site.xml,
   * when remote metastore is used.
   *
   * @throws IOException
   * @throws Throwable
   */
  @Test
  public void testHiveMetastoreRemoteConfig() throws IOException, Throwable {
    // switch to hive-site.xml with remote metastore
    setHiveSiteWithRemoteMetastore();

    // Set HiveConf statics to default values
    resetDefaults();

    // create hiveconf again to run initialization code, to see if value changes
    HiveConf conf = new HiveConf();

    // check the properties expected in hive client without metastore
    verifyMetastoreConfNotLoaded(conf);
    assertEquals("from.hive-site.xml",
        conf.get("hive.dummyparam.test.server.specific.config.override"));

    // get HS2 site.xml loaded
    new HiveServer2();
    conf = new HiveConf();
    verifyHS2ConfParams(conf);
    verifyMetastoreConfNotLoaded(conf);
  }

  private void verifyMetastoreConfNotLoaded(HiveConf conf) {
    assertFalse(HiveConf.isLoadMetastoreConfig());
    assertNull(conf.get("hive.dummyparam.test.server.specific.config.metastoresite"));
  }

  /**
   * Set new hive-site.xml file location that has remote metastore config
   *
   * @throws IOException
   */
  private void setHiveSiteWithRemoteMetastore() throws IOException {
    // new *hive-site.xml file
    String newConfFile = System.getProperty("test.tmp.dir") + File.separator
        + this.getClass().getSimpleName() + "hive-site.xml";

    // create a new conf file, using contents from current one
    // modifying the meastore.uri property
    File hiveSite = new File(newConfFile);
    FileOutputStream out = new FileOutputStream(hiveSite);
    HiveConf.setHiveSiteLocation(oldDefaultHiveSite);
    HiveConf defaultHiveConf = new HiveConf();
    defaultHiveConf.setVar(ConfVars.METASTORE_URIS, "dummyvalue");
    // reset to the hive-site.xml values for following param
    defaultHiveConf.set("hive.dummyparam.test.server.specific.config.override",
        "from.hive-site.xml");
    defaultHiveConf.unset("hive.dummyparam.test.server.specific.config.metastoresite");
    defaultHiveConf.writeXml(out);

    // set the new hive-site.xml
    HiveConf.setHiveSiteLocation(hiveSite.toURI().toURL());
  }

}