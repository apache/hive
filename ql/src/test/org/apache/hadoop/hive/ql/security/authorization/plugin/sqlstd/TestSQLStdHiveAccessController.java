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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * Test SQLStdHiveAccessController
 */
public class TestSQLStdHiveAccessController {

  /**
   * Test if SQLStdHiveAccessController is applying configuration security
   * policy on hiveconf correctly
   *
   * @throws HiveAuthzPluginException
   */
  @Test
  public void checkConfigProcessing() throws HiveAuthzPluginException {
    HiveConf processedConf = new HiveConf();

    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator());
    accessController.applyAuthorizationConfigPolicy(processedConf);

    // check that unsafe commands have been disabled
    assertEquals("only set command should be allowed",
        processedConf.getVar(ConfVars.HIVE_SECURITY_COMMAND_WHITELIST), "set");

    // check that hook to disable transforms has been added
    assertTrue("Check for transform query disabling hook",
        processedConf.getVar(ConfVars.PREEXECHOOKS).contains(DisallowTransformHook.class.getName()));

    verifyParamSettability(SQLStdHiveAccessController.defaultModWhiteListSqlStdAuth, processedConf);

  }

  /**
   * Verify that params in settableParams can be modified, and other random ones can't be modified
   * @param settableParams
   * @param processedConf
   */
  private void verifyParamSettability(String [] settableParams, HiveConf processedConf) {
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
   * Test that modifying HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST config works
   * @throws HiveAuthzPluginException
   */
  @Test
  public void checkConfigProcessingCustomSetWhitelist() throws HiveAuthzPluginException {

    HiveConf processedConf = new HiveConf();
    // add custom value, including one from the default, one new one
    String [] settableParams = {SQLStdHiveAccessController.defaultModWhiteListSqlStdAuth[0], "abcs.dummy.test.param"};
    processedConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST,
        Joiner.on(",").join(settableParams));


    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator());
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
