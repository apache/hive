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
package org.apache.hadoop.hive.conf;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
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
    checkHadoopConf(ConfVars.HIVETESTMODEDUMMYSTATAGGR.varname, null);
    checkConfVar(ConfVars.HIVETESTMODEDUMMYSTATAGGR, "");
    checkHiveConf(ConfVars.HIVETESTMODEDUMMYSTATAGGR.varname, "value2");

    // Property defined in hive-site.xml only
    checkHadoopConf("test.property1", null);
    checkHiveConf("test.property1", "value1");

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
    Assert.assertEquals(TimeUnit.MICROSECONDS, HiveConf.unitFor("usecs", null));
    Assert.assertEquals(TimeUnit.NANOSECONDS, HiveConf.unitFor("ns", null));
    Assert.assertEquals(TimeUnit.NANOSECONDS, HiveConf.unitFor("nsecs", null));
  }

  @Test
  public void testToSizeBytes() throws Exception {
    Assert.assertEquals(1L, HiveConf.toSizeBytes("1b"));
    Assert.assertEquals(1L, HiveConf.toSizeBytes("1bytes"));
    Assert.assertEquals(1024L, HiveConf.toSizeBytes("1kb"));
    Assert.assertEquals(1048576L, HiveConf.toSizeBytes("1mb"));
    Assert.assertEquals(1073741824L, HiveConf.toSizeBytes("1gb"));
    Assert.assertEquals(1099511627776L, HiveConf.toSizeBytes("1tb"));
    Assert.assertEquals(1125899906842624L, HiveConf.toSizeBytes("1pb"));
  }

  @Test
  public void testHiddenConfig() throws Exception {
    HiveConf conf = new HiveConf();

    // check that a change to the hidden list should fail
    try {
      final String name = HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname;
      conf.verifyAndSet(name, "");
      conf.verifyAndSet(name + "postfix", "");
      Assert.fail("Setting config property " + name + " should fail");
    } catch (IllegalArgumentException e) {
      // the verifyAndSet in this case is expected to fail with the IllegalArgumentException
    }

    ArrayList<String> hiddenList = Lists.newArrayList(
        HiveConf.ConfVars.METASTOREPWD.varname,
        HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname,
        "fs.s3.awsSecretAccessKey",
        "fs.s3n.awsSecretAccessKey",
        "dfs.adls.oauth2.credential",
        "fs.adl.oauth2.credential"
    );

    for (String hiddenConfig : hiddenList) {
      // check configs are hidden
      Assert.assertTrue("config " + hiddenConfig + " should be hidden",
          conf.isHiddenConfig(hiddenConfig));
      // check stripHiddenConfigurations removes the property
      Configuration conf2 = new Configuration(conf);
      conf2.set(hiddenConfig, "password");
      conf.stripHiddenConfigurations(conf2);
      // check that a property that begins the same is also hidden
      Assert.assertTrue(conf.isHiddenConfig(
          hiddenConfig + "postfix"));
      // Check the stripped property is the empty string
      Assert.assertEquals("", conf2.get(hiddenConfig));
    }
  }

  @Test
  public void testSparkConfigUpdate(){
    HiveConf conf = new HiveConf();
    Assert.assertFalse(conf.getSparkConfigUpdated());

    conf.verifyAndSet("spark.master", "yarn");
    Assert.assertTrue(conf.getSparkConfigUpdated());
    conf.verifyAndSet("hive.execution.engine", "spark");
    Assert.assertTrue("Expected spark config updated.", conf.getSparkConfigUpdated());

    conf.setSparkConfigUpdated(false);
    Assert.assertFalse(conf.getSparkConfigUpdated());
  }
  @Test
  public void testEncodingDecoding() throws UnsupportedEncodingException {
    HiveConf conf = new HiveConf();
    String query = "select blah, '\u0001' from random_table";
    conf.setQueryString(query);
    Assert.assertEquals(URLEncoder.encode(query, "UTF-8"), conf.get(ConfVars.HIVEQUERYSTRING.varname));
    Assert.assertEquals(query, conf.getQueryString());
  }
}
