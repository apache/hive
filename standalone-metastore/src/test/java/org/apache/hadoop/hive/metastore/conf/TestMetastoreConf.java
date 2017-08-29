/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestMetastoreConf {

  private static final Logger LOG = LoggerFactory.getLogger(TestMetastoreConf.class);

  private Configuration conf;
  private Random rand = new Random();

  @After
  public void unsetProperties() {
    for (MetastoreConf.ConfVars var : MetastoreConf.dataNucleusAndJdoConfs) {
      System.getProperties().remove(var.varname);
    }
  }

  static class TestClass1 implements Runnable {
    @Override
    public void run() {

    }
  }

  static class TestClass2 implements Runnable {
    @Override
    public void run() {

    }
  }
  private void createConfFile(String fileName, boolean inConf, String envVar,
                              Map<String, String> properties) throws IOException {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File dir = new File(tmpDir, randomFileName());
    Assume.assumeTrue(dir.mkdir());
    dir.deleteOnExit();
    System.setProperty(MetastoreConf.TEST_ENV_WORKAROUND + envVar, dir.getAbsolutePath());
    if (inConf) {
      dir = new File(dir, "conf");
      Assume.assumeTrue(dir.mkdir());
      dir.deleteOnExit();
    }
    File confFile = new File(dir, fileName);
    confFile.deleteOnExit();
    FileWriter writer = new FileWriter(confFile);
    writer.write("<configuration>\n");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      writer.write("  <property>\n");
      writer.write("    <name>");
      writer.write(entry.getKey());
      writer.write("</name>\n");
      writer.write("    <value>");
      writer.write(entry.getValue());
      writer.write("</value>\n");
      writer.write("  </property>\n");
    }
    writer.write("</configuration>\n");
    writer.close();
  }

  private String randomFileName() {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      buf.append((char)(rand.nextInt(26) + 'a'));
    }
    return buf.toString();
  }

  private Map<String, String> instaMap(String... vals) {
    Map<String, String> properties = new HashMap<>(vals.length / 2);
    for (int i = 0; i < vals.length; i+= 2) {
      properties.put(vals[i], vals[i+1]);
    }
    return properties;
  }

  @Test
  public void defaults() {
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals("defaultval", MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals(42, MetastoreConf.getLongVar(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals(3.141592654, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.0000001);
    Assert.assertTrue(MetastoreConf.getBoolVar(conf, ConfVars.BOOLEAN_TEST_ENTRY));
    Assert.assertEquals(1, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY, TimeUnit.SECONDS));
    Assert.assertEquals(1000,
        MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY, TimeUnit.MILLISECONDS));
    Collection<String> list = MetastoreConf.getStringCollection(conf, ConfVars.STR_LIST_ENTRY);
    Assert.assertEquals(3, list.size());
    Assert.assertTrue(list.contains("a"));
    Assert.assertTrue(list.contains("b"));
    Assert.assertTrue(list.contains("c"));
    Assert.assertSame(TestClass1.class,
        MetastoreConf.getClass(conf, ConfVars.CLASS_TEST_ENTRY, TestClass1.class, Runnable.class));
    Assert.assertEquals("defaultval", MetastoreConf.get(conf, ConfVars.STR_TEST_ENTRY.varname));
    Assert.assertEquals("defaultval", MetastoreConf.get(conf, ConfVars.STR_TEST_ENTRY.hiveName));
    Assert.assertEquals("defaultval", MetastoreConf.getAsString(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals("42", MetastoreConf.getAsString(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals("3.141592654", MetastoreConf.getAsString(conf, ConfVars.DOUBLE_TEST_ENTRY));
    Assert.assertEquals("true", MetastoreConf.getAsString(conf, ConfVars.BOOLEAN_TEST_ENTRY));
  }

  @Test
  public void readMetastoreSiteWithMetastoreConfDir() throws IOException {
    createConfFile("metastore-site.xml", false, "METASTORE_CONF_DIR", instaMap(
        "test.str", "notthedefault",
        "test.long", "37",
        "test.double", "1.8",
        "test.bool", "false",
        "test.time", "30s",
        "test.str.list", "d,e",
        "test.class", TestClass2.class.getName()
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals("notthedefault", MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals(37L, MetastoreConf.getLongVar(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals(37, MetastoreConf.getIntVar(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals(1.8, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.01);
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.SECONDS));
    Assert.assertEquals(30000,
        MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY, TimeUnit.MILLISECONDS));
    Collection<String> list = MetastoreConf.getStringCollection(conf, ConfVars.STR_LIST_ENTRY);
    Assert.assertEquals(2, list.size());
    Assert.assertTrue(list.contains("d"));
    Assert.assertTrue(list.contains("e"));
    Assert.assertSame(TestClass2.class,
        MetastoreConf.getClass(conf, ConfVars.CLASS_TEST_ENTRY, TestClass1.class, Runnable.class));
    Assert.assertEquals("1.8", MetastoreConf.get(conf, ConfVars.DOUBLE_TEST_ENTRY.varname));
    Assert.assertEquals("1.8", MetastoreConf.get(conf, ConfVars.DOUBLE_TEST_ENTRY.hiveName));
    Assert.assertEquals("notthedefault", MetastoreConf.getAsString(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals("37", MetastoreConf.getAsString(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals("1.8", MetastoreConf.getAsString(conf, ConfVars.DOUBLE_TEST_ENTRY));
    Assert.assertEquals("false", MetastoreConf.getAsString(conf, ConfVars.BOOLEAN_TEST_ENTRY));
  }

  @Test
  public void readMetastoreSiteWithMetastoreHomeDir() throws IOException {
    createConfFile("metastore-site.xml", true, "METASTORE_HOME", instaMap(
        "test.long", "24"
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals(24, MetastoreConf.getLongVar(conf, ConfVars.LONG_TEST_ENTRY));
  }

  @Ignore // Ignore for now as Hive's tests create a hive-site.xml in the test directory
  public void readHiveSiteWithHiveConfDir() throws IOException {
    createConfFile("hive-site.xml", false, "HIVE_CONF_DIR", instaMap(
        "test.double", "1.8"
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals(1.8, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.01);
  }

  @Ignore // Ignore for now as Hive's tests create a hive-site.xml in the test directory
  public void readHiveSiteWithHiveHomeDir() throws IOException {
    createConfFile("hive-site.xml", true, "HIVE_HOME", instaMap(
        "test.bool", "false"
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertFalse(MetastoreConf.getBoolVar(conf, ConfVars.BOOLEAN_TEST_ENTRY));
  }

  @Ignore // Ignore for now as Hive's tests create a hive-metastoresite.xml in the test directory
  public void readHiveMetastoreSiteWithHiveConfDir() throws IOException {
    createConfFile("hivemetastore-site.xml", false, "HIVE_CONF_DIR", instaMap(
        "test.double", "1.8"
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals(1.8, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.01);
  }

  @Ignore // Ignore for now as Hive's tests create a hive-metastoresite.xml in the test directory
  public void readHiveMetastoreSiteWithHiveHomeDir() throws IOException {
    createConfFile("hivemetastore-site.xml", true, "HIVE_HOME", instaMap(
        "test.bool", "false"
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertFalse(MetastoreConf.getBoolVar(conf, ConfVars.BOOLEAN_TEST_ENTRY));
  }

  @Test
  public void setAndRead() throws IOException {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.STR_TEST_ENTRY, "notthedefault");
    Assert.assertEquals("notthedefault", MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY));

    MetastoreConf.setDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY, 1.8);
    Assert.assertEquals(1.8, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.01);

    MetastoreConf.setLongVar(conf, ConfVars.LONG_TEST_ENTRY, 24);
    Assert.assertEquals(24, MetastoreConf.getLongVar(conf, ConfVars.LONG_TEST_ENTRY));

    MetastoreConf.setTimeVar(conf, ConfVars.TIME_TEST_ENTRY, 5, TimeUnit.MINUTES);
    Assert.assertEquals(300, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.SECONDS));
    Assert.assertEquals(300000,
        MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY, TimeUnit.MILLISECONDS));
  }

  @Test
  public void valuesSetFromProperties() {
    try {
      System.setProperty(MetastoreConf.ConfVars.STR_TEST_ENTRY.varname, "from-properties");
      conf = MetastoreConf.newMetastoreConf();
      Assert.assertEquals("from-properties", MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY));
    } finally {
      System.getProperties().remove(MetastoreConf.ConfVars.STR_TEST_ENTRY.varname);
    }
  }

  @After
  public void unsetEnvWorkAround() {
    // We have to unset the env workarounds so they don't confuse each other between tests.
    System.getProperties().remove(MetastoreConf.TEST_ENV_WORKAROUND + "METASTORE_CONF_DIR");
    System.getProperties().remove(MetastoreConf.TEST_ENV_WORKAROUND + "METASTORE_HOME");
    System.getProperties().remove(MetastoreConf.TEST_ENV_WORKAROUND + "HIVE_CONF_DIR");
    System.getProperties().remove(MetastoreConf.TEST_ENV_WORKAROUND + "HIVE_HOME");
  }

  @Test
  public void hiveNames() throws IOException {
    createConfFile("metastore-site.xml", false, "METASTORE_CONF_DIR", instaMap(
        "hive.test.str", "hivedefault",
        "hive.test.double", "1.9",
        "hive.test.long", "89",
        "hive.test.bool", "false",
        "hive.test.time", "3s",
        "hive.test.str.list", "g,h,i,j",
        "hive.test.class", TestClass2.class.getName()
    ));
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals("hivedefault", MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals(1.9, MetastoreConf.getDoubleVar(conf, ConfVars.DOUBLE_TEST_ENTRY),
        0.01);
    Assert.assertEquals(89L, MetastoreConf.getLongVar(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals(89, MetastoreConf.getIntVar(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals(3, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.SECONDS));
    Assert.assertEquals(3000,
        MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY, TimeUnit.MILLISECONDS));
    Collection<String> list = MetastoreConf.getStringCollection(conf, ConfVars.STR_LIST_ENTRY);
    Assert.assertEquals(4, list.size());
    Assert.assertTrue(list.contains("g"));
    Assert.assertTrue(list.contains("h"));
    Assert.assertTrue(list.contains("i"));
    Assert.assertTrue(list.contains("j"));
    Assert.assertSame(TestClass2.class,
        MetastoreConf.getClass(conf, ConfVars.CLASS_TEST_ENTRY, TestClass1.class, Runnable.class));
    Assert.assertEquals("3s", MetastoreConf.get(conf, ConfVars.TIME_TEST_ENTRY.varname));
    Assert.assertEquals("3s", MetastoreConf.get(conf, ConfVars.TIME_TEST_ENTRY.hiveName));
    Assert.assertEquals("hivedefault", MetastoreConf.getAsString(conf, ConfVars.STR_TEST_ENTRY));
    Assert.assertEquals("89", MetastoreConf.getAsString(conf, ConfVars.LONG_TEST_ENTRY));
    Assert.assertEquals("1.9", MetastoreConf.getAsString(conf, ConfVars.DOUBLE_TEST_ENTRY));
    Assert.assertEquals("false", MetastoreConf.getAsString(conf, ConfVars.BOOLEAN_TEST_ENTRY));
  }

  @Test
  public void timeUnits() throws IOException {
    conf = MetastoreConf.newMetastoreConf();

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30s");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.SECONDS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30seconds");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.SECONDS));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30ms");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MILLISECONDS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30msec");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MILLISECONDS));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30us");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MICROSECONDS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30usec");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MICROSECONDS));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30m");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MINUTES));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30minutes");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.MINUTES));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30ns");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.NANOSECONDS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30nsec");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.NANOSECONDS));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30h");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.HOURS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30hours");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.HOURS));

    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30d");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.DAYS));
    conf.set(MetastoreConf.ConfVars.TIME_TEST_ENTRY.varname, "30days");
    Assert.assertEquals(30, MetastoreConf.getTimeVar(conf, ConfVars.TIME_TEST_ENTRY,
        TimeUnit.DAYS));
  }

  @Test
  public void passedInDefaults() {
    conf = MetastoreConf.newMetastoreConf();
    Assert.assertEquals("passed-in-defaultval",
        MetastoreConf.getVar(conf, ConfVars.STR_TEST_ENTRY, "passed-in-defaultval"));

  }

  @Test
  public void validValidations() {
    ConfVars.STR_SET_ENTRY.validate("a");
    ConfVars.TIME_TEST_ENTRY.validate("1");
    ConfVars.TIME_VALIDATOR_ENTRY_INCLUSIVE.validate("500ms");
    ConfVars.TIME_VALIDATOR_ENTRY_INCLUSIVE.validate("1500ms");
    ConfVars.TIME_VALIDATOR_ENTRY_INCLUSIVE.validate("1000ms");
    ConfVars.TIME_VALIDATOR_ENTRY_EXCLUSIVE.validate("1000ms");
  }

  @Test(expected = IllegalArgumentException.class)
  public void badSetEntry() {
    ConfVars.STR_SET_ENTRY.validate("d");
  }

  @Test(expected = IllegalArgumentException.class)
  public void badTimeEntry() {
    ConfVars.TIME_TEST_ENTRY.validate("1x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void timeOutsideInclusive() {
    ConfVars.TIME_VALIDATOR_ENTRY_INCLUSIVE.validate("1day");
  }

  @Test(expected = IllegalArgumentException.class)
  public void timeMinExclusive() {
    ConfVars.TIME_VALIDATOR_ENTRY_EXCLUSIVE.validate("500ms");
  }

  @Test(expected = IllegalArgumentException.class)
  public void timeMaxExclusive() {
    ConfVars.TIME_VALIDATOR_ENTRY_EXCLUSIVE.validate("1500ms");
  }

  @Test(expected = IllegalArgumentException.class)
  public void timeOutsideExclusive() {
    ConfVars.TIME_VALIDATOR_ENTRY_EXCLUSIVE.validate("1min");
  }

  @Test
  public void unprintable() {
    Assert.assertTrue(MetastoreConf.isPrintable(ConfVars.STR_TEST_ENTRY.varname));
    Assert.assertFalse(MetastoreConf.isPrintable(ConfVars.PWD.varname));
    Assert.assertFalse(MetastoreConf.isPrintable(ConfVars.PWD.hiveName));
  }

  @Test
  public void unsetValues() {
    conf = MetastoreConf.newMetastoreConf();
    conf.set("a.random.key", "abc");
    Assert.assertNull(MetastoreConf.get(conf, "no.such.key.ever"));
    Assert.assertEquals("abc", MetastoreConf.get(conf, "a.random.key"));
  }

}
