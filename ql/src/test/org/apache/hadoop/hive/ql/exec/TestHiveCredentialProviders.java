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
package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.apache.hadoop.hive.conf.Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR;
import static org.apache.hadoop.hive.conf.Constants.HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG;
import static org.apache.hadoop.hive.conf.Constants.HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestHiveCredentialProviders {
  private static final String HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL = "testhadoopCredStorePassword";
  private static final String HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL = "testhiveJobCredPassword";
  private static final String JOB_CREDSTORE_LOCATION = "jceks://hdfs/user/hive/creds.jceks";
  private static final String HADOOP_CREDSTORE_LOCATION =
      "localjceks://file/user/hive/localcreds.jceks";

  private static final Collection<String> REDACTED_PROPERTIES = Arrays.asList(
      JobConf.MAPRED_MAP_TASK_ENV,
      JobConf.MAPRED_REDUCE_TASK_ENV,
      MRJobConfig.MR_AM_ADMIN_USER_ENV,
      TezConfiguration.TEZ_AM_LAUNCH_ENV);

  private Configuration jobConf;

  /*
   * Dirty hack to set the environment variables using reflection code. This method is for testing
   * purposes only and should not be used elsewhere
   */
  private final static void setEnv(Map<String, String> newenv) throws Exception {
    Class[] classes = Collections.class.getDeclaredClasses();
    Map<String, String> env = System.getenv();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object obj = field.get(env);
        Map<String, String> map = (Map<String, String>) obj;
        map.clear();
        map.putAll(newenv);
      }
    }
  }

  @Before
  public void resetConfig() {
    jobConf = new JobConf();
  }
  /*
   * Tests whether credential provider is updated when HIVE_JOB_CREDSTORE_PASSWORD is set and when
   * hiveConf sets HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDSTORE_LOCATION
   *
   * JobConf should contain the mapred env variable equal to ${HIVE_JOB_CREDSTORE_PASSWORD} and the
   * hadoop.security.credential.provider.path property should be equal to value of
   * HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDSTORE_LOCATION
   */
  @Test
  public void testJobCredentialProvider() throws Exception {
    setupConfigs(true, true, true, true);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    // make sure credential provider path points to HIVE_SERVER2_JOB_CREDSTORE_LOCATION
    Assert.assertEquals(JOB_CREDSTORE_LOCATION,
        jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));

    // make sure MAP task environment points to HIVE_JOB_CREDSTORE_PASSWORD
    Assert.assertEquals(HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    // make sure REDUCE task environment points to HIVE_JOB_CREDSTORE_PASSWORD
    Assert.assertEquals(HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    // make sure TEZ AM environment points to HIVE_JOB_CREDSTORE_PASSWORD
    Assert.assertEquals(HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertTrue(jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
        .containsAll(REDACTED_PROPERTIES));
  }

  /*
   * If hive job credstore location is not set, but hadoop credential provider is set
   * jobConf should contain hadoop credstore location and password should be from HADOOP_CREDSTORE_PASSWORD
   */
  @Test
  public void testHadoopCredentialProvider() throws Exception {
    setupConfigs(true, true, true, false);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    Assert.assertEquals(HADOOP_CREDSTORE_LOCATION,
        jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));

    // make sure MAP task environment points to HADOOP_CREDSTORE_PASSWORD
    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    // make sure REDUCE task environment points to HADOOP_CREDSTORE_PASSWORD
    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    // make sure TEZ AM environment points to HADOOP_CREDSTORE_PASSWORD
    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertTrue(jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
        .containsAll(REDACTED_PROPERTIES));
  }

  /*
   * If there is no credential provider configured for hadoop, jobConf should not contain
   * credstore password and provider path even if HIVE_JOB_CRESTORE_PASSWORD env is set
   */
  @Test
  public void testNoCredentialProviderWithPassword() throws Exception {
    setupConfigs(false, false, true, false);

    Assert.assertTrue(StringUtils.isBlank(jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG)));

    Assert.assertNull(getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertNull(getValueFromJobConf(jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertNull(getValueFromJobConf(jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertNull(getValueFromJobConf(jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    REDACTED_PROPERTIES.forEach(property -> Assert.assertFalse(
        jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
            .contains(property)));
  }

  /*
   * If hive job credential provider is set but HIVE_JOB_CREDSTORE_PASSWORD is not set, use
   * HADOOP_CREDSTORE_PASSWORD in the jobConf
   */
  @Test
  public void testJobCredentialProviderWithDefaultPassword() throws Exception {
    setupConfigs(false, true, false, true);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    Assert.assertEquals(JOB_CREDSTORE_LOCATION,
        jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertTrue(jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
        .containsAll(REDACTED_PROPERTIES));
  }

  /*
   * When neither HADOOP_CREDSTORE_PASSWORD nor HIVE_JOB_CREDSTORE_PASSWORD
   * are not set jobConf should contain only the credential provider path
   */
  @Test
  public void testCredentialProviderWithNoPasswords() throws Exception {
    setupConfigs(true, false, false, true);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    Assert.assertEquals(JOB_CREDSTORE_LOCATION,
        jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));
    Assert.assertNull(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV));
    Assert.assertNull(jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV));
    Assert.assertNull(jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV));
    Assert.assertNull(jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV));

    REDACTED_PROPERTIES.forEach(property -> Assert.assertFalse(
        jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
            .contains(property)));

    resetConfig();
    setupConfigs(true, false, false, false);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    Assert.assertEquals(HADOOP_CREDSTORE_LOCATION,
        jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));
    Assert.assertNull(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV));
    Assert.assertNull(jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV));
    Assert.assertNull(jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV));
    Assert.assertNull(jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV));

    REDACTED_PROPERTIES.forEach(property -> Assert.assertFalse(
        jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
            .contains(property)));
  }

  /*
   * default behavior when neither hive.job.credstore location is set nor
   * HIVE_JOB_CREDSTORE_PASSWORD is. In this case if hadoop credential provider is configured job
   * config should use that else it should remain unset
   */
  @Test
  public void testJobCredentialProviderUnset() throws Exception {
    setupConfigs(true, true, false, false);

    HiveConfUtil.updateJobCredentialProviders(jobConf);
    assertEquals(HADOOP_CREDSTORE_LOCATION, jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG));

    assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, getValueFromJobConf(
        jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV), HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    Assert.assertTrue(jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
        .containsAll(REDACTED_PROPERTIES));
  }

  /*
   * Test the unsecure base case when neither hadoop nor job-specific
   * credential provider is set
   */
  @Test
  public void testNoCredentialProvider() throws Exception {
    setupConfigs(false, false, false, false);

    assertTrue(StringUtils.isBlank(jobConf.get(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG)));

    assertNull(getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    assertNull(getValueFromJobConf(jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    assertNull(getValueFromJobConf(jobConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    assertNull(getValueFromJobConf(jobConf.get(TezConfiguration.TEZ_AM_LAUNCH_ENV),
        HADOOP_CREDENTIAL_PASSWORD_ENVVAR));

    REDACTED_PROPERTIES.forEach(property -> Assert.assertFalse(
        jobConf.getStringCollection(MRJobConfig.MR_JOB_REDACTED_PROPERTIES)
            .contains(property)));
  }

  /*
   * Test updateCredentialProviders does not corrupt existing values of
   * Mapred env configs
   */
  @Test
  public void testExistingConfiguration() throws Exception {
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "k1=v1, k2=v2, HADOOP_CREDSTORE_PASSWORD=test");
    setupConfigs(false, true, false, true);
    HiveConfUtil.updateJobCredentialProviders(jobConf);

    assertEquals("v1", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k1"));
    assertEquals("v2", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k2"));

    resetConfig();

    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "k1=v1, HADOOP_CREDSTORE_PASSWORD=test, k2=v2");
    setupConfigs(false, true, false, true);
    HiveConfUtil.updateJobCredentialProviders(jobConf);

    assertEquals("v1", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k1"));
    assertEquals("v2", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k2"));

    resetConfig();
    jobConf.set(JobConf.MAPRED_MAP_TASK_ENV, "HADOOP_CREDSTORE_PASSWORD=test, k1=v1, k2=v2");
    setupConfigs(false, true, false, true);
    HiveConfUtil.updateJobCredentialProviders(jobConf);

    assertEquals("v1", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k1"));
    assertEquals("v2", getValueFromJobConf(jobConf.get(JobConf.MAPRED_MAP_TASK_ENV), "k2"));
  }

  /**
   * Sets up the environment and configurations
   *
   * @param setHadoopCredProvider set hadoop credstore provider path
   * @param setHadoopCredstorePassword set HADOOP_CREDSTORE_PASSWORD env variable
   * @param setHiveCredPassword set HIVE_JOB_CREDSTORE_PASSWORD env variable
   * @param setHiveProviderPath set HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDSTORE_LOCATION in the
   *          hive config
   * @throws Exception
   */
  private void setupConfigs(boolean setHadoopCredProvider, boolean setHadoopCredstorePassword,
      boolean setHiveCredPassword, boolean setHiveProviderPath) throws Exception {
    Map<String, String> mockEnv = new HashMap<>();
    // sets the env variable HADOOP_CREDSTORE_PASSWORD to value defined by HADOOP_CREDSTORE_PASSWORD
    // sets hadoop.security.credential.provider.path property to simulate default credential
    // provider setup
    if (setHadoopCredProvider) {
      jobConf.set(HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG, HADOOP_CREDSTORE_LOCATION);
    }
    if (setHadoopCredstorePassword) {
      mockEnv.put(HADOOP_CREDENTIAL_PASSWORD_ENVVAR, HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL);
    }
    // sets the env variable HIVE_JOB_CREDSTORE_PASSWORD to value defined by
    // HIVE_JOB_CREDSTORE_PASSWORD
    if (setHiveCredPassword) {
      mockEnv.put(HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR, HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL);
    }
    TestHiveCredentialProviders.setEnv(mockEnv);
    // set hive provider path in hiveConf if setHiveProviderPath is true
    // simulates hive.server2.job.credstore.location property set in hive-site.xml/core-site.xml of
    // HS2
    if (setHiveProviderPath) {
      jobConf.set(HiveConf.ConfVars.HIVE_SERVER2_JOB_CREDENTIAL_PROVIDER_PATH.varname,
          JOB_CREDSTORE_LOCATION);
    }
    jobConf.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
  }

  /*
   * Extract value from a comma-separated key=value pairs
   */
  private String getValueFromJobConf(String keyValuePairs, String key) {
    if (keyValuePairs == null) {
      return null;
    }
    String[] keyValues = keyValuePairs.split(",");
    for (String kv : keyValues) {
      String[] parts = kv.split("=");
      if (key.equals(parts[0].trim())) {
        return parts[1].trim();
      }
    }
    return null;
  }

  /*
   * Test if the environment variables can be set. If this test fails
   * all the other tests will also fail because environment is not getting setup
   */
  @Test
  public void testEnv() throws Exception {
    Map<String, String> mockEnv = new HashMap<>();
    mockEnv.put(HADOOP_CREDENTIAL_PASSWORD_ENVVAR, HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL);
    mockEnv.put(HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR, HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL);
    TestHiveCredentialProviders.setEnv(mockEnv);
    assertEquals(HADOOP_CREDSTORE_PASSWORD_ENVVAR_VAL, System.getenv(HADOOP_CREDENTIAL_PASSWORD_ENVVAR));
    assertEquals(HIVE_JOB_CREDSTORE_PASSWORD_ENVVAR_VAL, System.getenv(HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR));
  }
}
