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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.Builder;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * Test SQLStdHiveAccessController
 */
public class TestSQLStdHiveAccessControllerHS2 {

  /**
   * Test if SQLStdHiveAccessController is applying configuration security
   * policy on hiveconf correctly
   *
   * @throws HiveAuthzPluginException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   * @throws IllegalArgumentException
   * @throws SecurityException
   */
  @Test
  public void testConfigProcessing() throws HiveAuthzPluginException, SecurityException,
      IllegalArgumentException, NoSuchFieldException, IllegalAccessException {
    HiveConf processedConf = newAuthEnabledConf();
    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator(), getHS2SessionCtx());
    accessController.applyAuthorizationConfigPolicy(processedConf);

    // check that hook to disable transforms has been added
    assertTrue("Check for transform query disabling hook",
        processedConf.getVar(ConfVars.PRE_EXEC_HOOKS).contains(DisallowTransformHook.class.getName()));

    List<String> settableParams = getSettableParams();
    verifyParamSettability(settableParams, processedConf);

  }

  private HiveConf newAuthEnabledConf() {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    return conf;
  }

  /**
   * @return list of parameters that should be possible to set
   */
  private List<String> getSettableParams() throws SecurityException, NoSuchFieldException,
      IllegalArgumentException, IllegalAccessException {
    // get all the variable names being converted to regex in HiveConf, using reflection
    Field varNameField = HiveConf.class.getDeclaredField("SQL_STD_AUTH_SAFE_VAR_NAMES");
    varNameField.setAccessible(true);
    List<String> confVarList = Arrays.asList((String[]) varNameField.get(null));

    // create list with variables that match some of the regexes
    List<String> confVarRegexList = Arrays.asList("hive.convert.join.bucket.mapjoin.tez",
        "hive.optimize.index.filter.compact.maxsize", "hive.tez.dummy", "tez.task.dummy",
        "hive.exec.dynamic.partition", "hive.exec.dynamic.partition.mode",
        "hive.exec.max.dynamic.partitions", "hive.exec.max.dynamic.partitions.pernode",
        "oozie.HadoopAccessorService.created", "tez.queue.name","hive.druid.select.distribute",
        "distcp.options.px", "hive.materializedview.rewriting");

    // combine two lists
    List<String> varList = new ArrayList<String>();
    varList.addAll(confVarList);
    varList.addAll(confVarRegexList);
    return varList;

  }

  private HiveAuthzSessionContext getHS2SessionCtx() {
    Builder ctxBuilder = new HiveAuthzSessionContext.Builder();
    ctxBuilder.setClientType(CLIENT_TYPE.HIVESERVER2);
    return ctxBuilder.build();
  }

  /**
   * Verify that params in settableParams can be modified, and other random ones can't be modified
   * @param settableParams
   * @param processedConf
   */
  private void verifyParamSettability(List<String> settableParams, HiveConf processedConf) {
    // verify that the whitlelist params can be set
    for (String param : settableParams) {
      try {
        processedConf.verifyAndSet(param, "dummy");
      } catch (IllegalArgumentException e) {
        fail("Unable to set value for parameter in whitelist " + param + " " + e);
      }
    }

    // verify that non whitelist params can't be set
    assertConfModificationException(processedConf, "dummy.param");
    // does not make sense to have any of the metastore config variables to be
    // modifiable
    for (ConfVars metaVar : HiveConf.metaVars) {
      assertConfModificationException(processedConf, metaVar.varname);
    }
  }

  /**
   * Test that setting HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND config works
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessingCustomSetWhitelistAppend() throws HiveAuthzPluginException {
    // append new config params to whitelist
    List<String> paramRegexes = Arrays.asList("hive.ctest.param", "hive.abc..*");
    List<String> settableParams = Arrays.asList("hive.ctest.param", "hive.abc.def");
    verifySettability(paramRegexes, settableParams,
        ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND);
  }

  /**
   * Test that setting HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST config works
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessingCustomSetWhitelist() throws HiveAuthzPluginException {
    // append new config params to whitelist
    List<String> paramRegexes = Arrays.asList("hive.ctest.param", "hive.abc..*");
    List<String> settableParams = Arrays.asList("hive.ctest.param", "hive.abc.def");
    verifySettability(paramRegexes, settableParams,
        ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST);
  }

  private void verifySettability(List<String> paramRegexes, List<String> settableParams,
      ConfVars whiteListParam) throws HiveAuthzPluginException {
    HiveConf processedConf = newAuthEnabledConf();
    processedConf.setVar(whiteListParam,
        Joiner.on("|").join(paramRegexes));

    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator(), getHS2SessionCtx());
    accessController.applyAuthorizationConfigPolicy(processedConf);

    verifyParamSettability(settableParams, processedConf);
  }

  private void assertConfModificationException(HiveConf processedConf, String param) {
    boolean caughtEx = false;
    try {
      processedConf.verifyAndSet(param, "dummy");
    } catch (IllegalArgumentException e) {
      caughtEx = true;
    }
    assertTrue("Exception should be thrown while modifying the param " + param, caughtEx);
  }


}
