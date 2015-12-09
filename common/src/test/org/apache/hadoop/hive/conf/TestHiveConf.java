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
package org.apache.hadoop.hive.conf;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * TestHiveConf
 *
 * Test cases for HiveConf. Loads configuration files located
 * in common/src/test/resources.
 */
public class TestHiveConf {
  @Test
  public void testHiveSitePath() throws Exception {
    String expectedPath = HiveTestUtils.getFileFromClasspath("hive-site.xml");
    String hiveSiteLocation = HiveConf.getHiveSiteLocation().getPath();
    if (Shell.WINDOWS) {
      // Do case-insensitive comparison on Windows, as drive letter can have different case.
      expectedPath = expectedPath.toLowerCase();
      hiveSiteLocation = hiveSiteLocation.toLowerCase();
    }
    Assert.assertEquals(expectedPath, hiveSiteLocation);
  }

  private void checkHadoopConf(String name, String expectedHadoopVal) throws Exception {
    Assert.assertEquals(expectedHadoopVal, new JobConf(HiveConf.class).get(name));
  }

  private void checkConfVar(ConfVars var, String expectedConfVarVal) throws Exception {
    Assert.assertEquals(expectedConfVarVal, var.getDefaultValue());
  }

  private void checkHiveConf(String name, String expectedHiveVal) throws Exception {
    Assert.assertEquals(expectedHiveVal, new HiveConf().get(name));
  }

  @Test
  public void testConfProperties() throws Exception {
    // Make sure null-valued ConfVar properties do not override the Hadoop Configuration
    // NOTE: Comment out the following test case for now until a better way to test is found,
    // as this test case cannot be reliably tested. The reason for this is that Hive does
    // overwrite fs.default.name in HiveConf if the property is set in system properties.
    // checkHadoopConf(ConfVars.HADOOPFS.varname, "core-site.xml");
    // checkConfVar(ConfVars.HADOOPFS, null);
    // checkHiveConf(ConfVars.HADOOPFS.varname, "core-site.xml");

    // Make sure non-null-valued ConfVar properties *do* override the Hadoop Configuration
    checkHadoopConf(ConfVars.HADOOPNUMREDUCERS.varname, "1");
    checkConfVar(ConfVars.HADOOPNUMREDUCERS, "-1");
    checkHiveConf(ConfVars.HADOOPNUMREDUCERS.varname, "-1");

    // Non-null ConfVar only defined in ConfVars
    checkHadoopConf(ConfVars.HIVESKEWJOINKEY.varname, null);
    checkConfVar(ConfVars.HIVESKEWJOINKEY, "100000");
    checkHiveConf(ConfVars.HIVESKEWJOINKEY.varname, "100000");

    // ConfVar overridden in in hive-site.xml
    checkHadoopConf(ConfVars.METASTORE_CONNECTION_DRIVER.varname, null);
    checkConfVar(ConfVars.METASTORE_CONNECTION_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
    checkHiveConf(ConfVars.METASTORE_CONNECTION_DRIVER.varname, "hive-site.xml");

    // Property defined in hive-site.xml only
    checkHadoopConf("test.property1", null);
    checkHiveConf("test.property1", "hive-site.xml");

    // Test HiveConf property variable substitution in hive-site.xml
    checkHiveConf("test.var.hiveconf.property", ConfVars.DEFAULTPARTITIONNAME.getDefaultValue());
  }

  @Test
  public void testColumnNameMapping() throws Exception {
    for (int i = 0 ; i < 20 ; i++ ){
      Assert.assertTrue(i == HiveConf.getPositionFromInternalName(HiveConf.getColumnInternalName(i)));
    }
  }

  @Test
  public void testUnitFor() throws Exception {
    Assert.assertEquals(TimeUnit.SECONDS, HiveConf.unitFor("L", TimeUnit.SECONDS));
    Assert.assertEquals(TimeUnit.MICROSECONDS, HiveConf.unitFor("", TimeUnit.MICROSECONDS));
    Assert.assertEquals(TimeUnit.DAYS, HiveConf.unitFor("d", null));
    Assert.assertEquals(TimeUnit.DAYS, HiveConf.unitFor("days", null));
    Assert.assertEquals(TimeUnit.HOURS, HiveConf.unitFor("h", null));
    Assert.assertEquals(TimeUnit.HOURS, HiveConf.unitFor("hours", null));
    Assert.assertEquals(TimeUnit.MINUTES, HiveConf.unitFor("m", null));
    Assert.assertEquals(TimeUnit.MINUTES, HiveConf.unitFor("minutes", null));
    Assert.assertEquals(TimeUnit.SECONDS, HiveConf.unitFor("s", null));
    Assert.assertEquals(TimeUnit.SECONDS, HiveConf.unitFor("seconds", null));
    Assert.assertEquals(TimeUnit.MILLISECONDS, HiveConf.unitFor("ms", null));
    Assert.assertEquals(TimeUnit.MILLISECONDS, HiveConf.unitFor("msecs", null));
    Assert.assertEquals(TimeUnit.MICROSECONDS, HiveConf.unitFor("us", null));
    Assert.assertEquals(TimeUnit.MICROSECONDS, HiveConf.unitFor("useconds", null));
    Assert.assertEquals(TimeUnit.NANOSECONDS, HiveConf.unitFor("ns", null));
    Assert.assertEquals(TimeUnit.NANOSECONDS, HiveConf.unitFor("nsecs", null));
  }

  @Test
  public void testHiddenConfig() throws Exception {
    HiveConf conf = new HiveConf();
    // check password configs are hidden
    Assert.assertTrue(conf.isHiddenConfig(HiveConf.ConfVars.METASTOREPWD.varname));
    Assert.assertTrue(conf.isHiddenConfig(
        HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname));
    // check change hidden list should fail
    try {
      final String name = HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname;
      conf.verifyAndSet(name, "");
      Assert.fail("Setting config property " + name + " should fail");
    } catch (IllegalArgumentException e) {
      // the verifyAndSet in this case is expected to fail with the IllegalArgumentException
    }
    // check stripHiddenConfigurations
    Configuration conf2 = new Configuration(conf);
    conf2.set(HiveConf.ConfVars.METASTOREPWD.varname, "password");
    conf2.set(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname, "password");
    conf.stripHiddenConfigurations(conf2);
    Assert.assertEquals("", conf2.get(HiveConf.ConfVars.METASTOREPWD.varname));
    Assert.assertEquals("", conf2.get(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname));
  }
}
