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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
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

    //Property defined for hive masking algorithm
    checkConfVar(ConfVars.HIVE_MASKING_ALGO, "sha256");
    checkHiveConf(ConfVars.HIVE_MASKING_ALGO.varname, "sha256");

    // Property defined in hive-site.xml only
    checkHadoopConf("test.property1", null);
    checkHiveConf("test.property1", "value1");

    // Test HiveConf property variable substitution in hive-site.xml
    checkHiveConf("test.var.hiveconf.property", ConfVars.DEFAULTPARTITIONNAME.getDefaultValue());

    // Test if all the LLAP conf vars are defined in LLAP daemon conf vars
    Set<String> llapConfSet = getLLAPConfVars();
    for (String varName : llapConfSet) {
      Assert.assertTrue(HiveConf.getLlapDaemonConfVars().contains(varName));
    }
  }

  private Set<String> getLLAPConfVars() {
    Set<String> llapVarsExclusionSet = getLLAPVarsExclusionSet();
    Set<String> llapConfSet = new HashSet<>();
    for(ConfVars var: ConfVars.values()) {
      if (var.name().startsWith("LLAP_") && !llapVarsExclusionSet.contains(var.varname)) {
        llapConfSet.add(var.varname);
      }
    }
    return llapConfSet;
  }

  /**
   * Add those conf vars to this exclusion set which are not part of LLAP Daemon Conf vars
   */
  private Set<String> getLLAPVarsExclusionSet() {
    Set<String> llapExclusionSet = new HashSet<>();
    llapExclusionSet.add(ConfVars.LLAP_IO_CACHE_ONLY.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ROW_WRAPPER_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ACID_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_TRACE_SIZE.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_TRACE_ALWAYS_DUMP.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_PREALLOCATE.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_MAPPED.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_MAPPED_PATH.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_DISCARD_METHOD.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_DEFRAG_HEADROOM.varname);
    llapExclusionSet.add(ConfVars.LLAP_ALLOCATOR_MAX_FORCE_EVICTED.varname);
    llapExclusionSet.add(ConfVars.LLAP_TRACK_CACHE_USAGE.varname);
    llapExclusionSet.add(ConfVars.LLAP_CACHE_DEFAULT_FS_FILE_ID.varname);
    llapExclusionSet.add(ConfVars.LLAP_CACHE_ENABLE_ORC_GAP_CACHE.varname);
    llapExclusionSet.add(ConfVars.LLAP_CACHE_HYDRATION_STRATEGY_CLASS.varname);
    llapExclusionSet.add(ConfVars.LLAP_CACHE_HYDRATION_SAVE_DIR.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_FORMATS.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_ALLOC_SIZE.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_VECTOR_SERDE_ASYNC_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_SLICE_ROW_COUNT.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_SLICE_LRR.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_VRB_QUEUE_LIMIT_MAX.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_VRB_QUEUE_LIMIT_MIN.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_CVB_BUFFERED_SIZE.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_PROACTIVE_EVICTION_SWEEP_INTERVAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_PROACTIVE_EVICTION_INSTANT_DEALLOC.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_CACHE_DELETEDELTAS.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_SHARE_OBJECT_POOLS.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_ALLOW_UBER.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_ENFORCE_TREE.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_ENFORCE_VECTORIZED.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_ENFORCE_STATS.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_MAX_INPUT.varname);
    llapExclusionSet.add(ConfVars.LLAP_AUTO_MAX_OUTPUT.varname);
    llapExclusionSet.add(ConfVars.LLAP_SKIP_COMPILE_UDF_CHECK.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXECUTION_MODE.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ETL_SKIP_FORMAT.varname);
    llapExclusionSet.add(ConfVars.LLAP_OBJECT_CACHE_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_IO_ENCODE_THREADPOOL_MULTIPLIER.varname);
    llapExclusionSet.add(ConfVars.LLAP_USE_KERBEROS.varname);
    llapExclusionSet.add(ConfVars.LLAP_WEBUI_SPNEGO_KEYTAB_FILE.varname);
    llapExclusionSet.add(ConfVars.LLAP_WEBUI_SPNEGO_PRINCIPAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_FS_KERBEROS_PRINCIPAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_FS_KERBEROS_KEYTAB_FILE.varname);
    llapExclusionSet.add(ConfVars.LLAP_ZKSM_ZK_SESSION_TIMEOUT.varname);
    llapExclusionSet.add(ConfVars.LLAP_ZK_REGISTRY_USER.varname);
    llapExclusionSet.add(ConfVars.LLAP_ZK_REGISTRY_NAMESPACE.varname);
    llapExclusionSet.add(ConfVars.LLAP_PLUGIN_ACL.varname);
    llapExclusionSet.add(ConfVars.LLAP_PLUGIN_ACL_DENY.varname);
    llapExclusionSet.add(ConfVars.LLAP_REMOTE_TOKEN_REQUIRES_SIGNING.varname);
    llapExclusionSet.add(ConfVars.LLAP_DELEGATION_TOKEN_RENEW_INTERVAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_PLUGIN_RPC_PORT.varname);
    llapExclusionSet.add(ConfVars.LLAP_PLUGIN_RPC_NUM_HANDLERS.varname);
    llapExclusionSet.add(ConfVars.LLAP_HDFS_PACKAGE_DIR.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_NM_ADDRESS.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_EXEC_USE_FQDN.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_TASK_SCHEDULER_TIMEOUT_SECONDS.varname);
    llapExclusionSet.add(ConfVars.LLAP_MAPJOIN_MEMORY_OVERSUBSCRIBE_FACTOR.varname);
    llapExclusionSet.add(ConfVars.LLAP_MEMORY_OVERSUBSCRIPTION_MAX_EXECUTORS_PER_QUERY.varname);
    llapExclusionSet.add(ConfVars.LLAP_MAPJOIN_MEMORY_MONITOR_CHECK_INTERVAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_AM_REPORTER_MAX_THREADS.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS.varname);
    llapExclusionSet.add(ConfVars.LLAP_PLUGIN_CLIENT_NUM_THREADS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER.varname);
    llapExclusionSet.add(ConfVars.LLAP_NODEHEALTHCHECKS_MINTASKS.varname);
    llapExclusionSet.add(ConfVars.LLAP_NODEHEALTHCHECKS_MININTERVALDURATION.varname);
    llapExclusionSet.add(ConfVars.LLAP_NODEHEALTHCHECKS_TASKTIMERATIO.varname);
    llapExclusionSet.add(ConfVars.LLAP_NODEHEALTHCHECKS_EXECUTORRATIO.varname);
    llapExclusionSet.add(ConfVars.LLAP_NODEHEALTHCHECKS_MAXNODES.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_PRINCIPAL.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_KEYTAB_FILE.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MAX_TIMEOUT_MS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_NODE_DISABLE_BACK_OFF_FACTOR.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_PREEMPT_INDEPENDENT.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_NUM_SCHEDULABLE_TASKS_PER_NODE.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_SCHEDULER_LOCALITY_DELAY.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_METRICS_TIMED_WINDOW_AVERAGE_DATA_POINTS.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_METRICS_TIMED_WINDOW_AVERAGE_WINDOW_LENGTH.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_METRICS_SIMPLE_AVERAGE_DATA_POINTS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_TIMEOUT_MS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_COMMUNICATOR_LISTENER_THREAD_COUNT.varname);
    llapExclusionSet.add(ConfVars.LLAP_MAX_CONCURRENT_REQUESTS_PER_NODE.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_SLEEP_BETWEEN_RETRIES_MS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_UMBILICAL_SERVER_PORT.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_WEB_XFRAME_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_WEB_XFRAME_VALUE.varname);
    llapExclusionSet.add(ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS.varname);
    llapExclusionSet.add(ConfVars.LLAP_SPLIT_LOCATION_PROVIDER_CLASS.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_OUTPUT_STREAM_TIMEOUT.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_SEND_BUFFER_SIZE.varname);
    llapExclusionSet.add(ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_MAX_PENDING_WRITES.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_SPLITS_TEMP_TABLE_STORAGE_FORMAT.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_USE_HYBRID_CALENDAR.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_DEPLOYMENT_SETUP_ENABLED.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_RPC_PORT.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_OUTPUT_SERVICE_PORT.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET_PROVIDER.varname);
    llapExclusionSet.add(ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_JWT_SHARED_SECRET.varname);
    llapExclusionSet.add(ConfVars.LLAP_ENABLE_GRACE_JOIN_IN_LLAP.varname);
    llapExclusionSet.add(ConfVars.LLAP_HS2_ENABLE_COORDINATOR.varname);
    llapExclusionSet.add(ConfVars.LLAP_COLLECT_LOCK_METRICS.varname);
    llapExclusionSet.add(ConfVars.LLAP_TASK_TIME_SUMMARY.varname);
    return llapExclusionSet;
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
        HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD.varname,
        "fs.s3.awsSecretAccessKey",
        "fs.s3n.awsSecretAccessKey",
        "dfs.adls.oauth2.credential",
        "fs.adl.oauth2.credential",
        "fs.azure.account.oauth2.client.secret"
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
  public void testEncodingDecoding() throws UnsupportedEncodingException {
    HiveConf conf = new HiveConf();
    String query = "select blah, '\u0001' from random_table";
    conf.setQueryString(query);
    Assert.assertEquals(URLEncoder.encode(query, "UTF-8"), conf.get(ConfVars.HIVEQUERYSTRING.varname));
    Assert.assertEquals(query, conf.getQueryString());
  }

  @Test
  public void testAdditionalConfigFiles() throws Exception{
    URL url = ClassLoader.getSystemResource("hive-site.xml");
    File fileHiveSite = new File(url.getPath());

    String parFolder = fileHiveSite.getParent();
    //back up hive-site.xml
    String bakHiveSiteFileName = parFolder + "/hive-site-bak.xml";
    File fileBakHiveSite = new File(bakHiveSiteFileName);
    FileUtils.copyFile(fileHiveSite, fileBakHiveSite);

    String content = FileUtils.readFileToString(fileHiveSite);
    content = content.substring(0, content.lastIndexOf("</configuration>"));

    String testHiveSiteString = content + "<property>\n" +
            " <name>HIVE_SERVER2_PLAIN_LDAP_DOMAIN</name>\n" +
            " <value>a.com</value>\n" +
            "</property>\n" +
            "\n" +
            " <property>\n" +
            "   <name>hive.additional.config.files</name>\n" +
            "   <value>ldap-site.xml,other.xml</value>\n" +
            "   <description>additional config dir for Hive to load</description>\n" +
            " </property>\n" +
            "\n" +
            "</configuration>";

    FileUtils.writeStringToFile(fileHiveSite, testHiveSiteString);

    String testLdapString = "<?xml version=\"1.0\"?>\n" +
            "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
            "<configuration>\n" +
            "  <property>\n" +
            "  <name>hive.server2.authentication.ldap.Domain</name>\n" +
            "  <value>b.com</value>\n" +
            "</property>\n" +
            "\n" +
            "</configuration>";


    String newFileName = parFolder+"/ldap-site.xml";
    File f2 = new File(newFileName);
    FileUtils.writeStringToFile(f2, testLdapString);

    HiveConf conf = new HiveConf();
    String val = conf.getVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_DOMAIN);
    Assert.assertEquals("b.com", val);
    //restore and clean up
    FileUtils.copyFile(fileBakHiveSite, fileHiveSite);
    f2.delete();
    fileBakHiveSite.delete();
  }
}
