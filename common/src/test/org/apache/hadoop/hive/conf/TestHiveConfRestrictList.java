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



import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 * HiveConfRestrictList Test.
 */
public class TestHiveConfRestrictList {

  private HiveConf conf = null;

  @Before
  public void setUp() throws Exception {

    System.setProperty(ConfVars.HIVE_CONF_RESTRICTED_LIST.varname,
        ConfVars.HIVE_TEST_MODE_PREFIX.varname);
    conf = new HiveConf();
  }

  /**
   * Test that configs in restrict list can't be changed
   * @throws Exception
   */
  @Test
  public void testRestriction() throws Exception {
    verifyRestriction(ConfVars.HIVE_TEST_MODE_PREFIX.varname, "foo");
    conf.verifyAndSet(ConfVars.HIVE_AM_SPLIT_GENERATION.varname, "false");
  }

  /**
   * Test that configs in restrict list can't be changed
   * @throws Exception
   */
  @Test
  public void testMultipleRestrictions() throws Exception {
    verifyRestriction(ConfVars.HIVE_TEST_MODE_PREFIX.varname, "foo");
    verifyRestriction(ConfVars.HIVE_IN_TEST.varname, "true");
  }

  /**
   * Test that restrict list config itself can't be changed
   * @throws Exception
   */
  @Test
  public void testRestrictList() throws Exception {
    verifyRestriction(ConfVars.HIVE_CONF_RESTRICTED_LIST.varname, "foo");
  }

  /**
   * Test appending new configs vars added to restrict list
   * @throws Exception
   */
  @Test
  public void testAppendRestriction() throws Exception {
    String appendListStr = ConfVars.SCRATCH_DIR.varname + "," +
        ConfVars.LOCAL_SCRATCH_DIR.varname + "," +
        ConfVars.METASTORE_URIS.varname;

    conf.addToRestrictList(appendListStr);
    // check if the new configs are added to HIVE_CONF_RESTRICTED_LIST
    String newRestrictList = conf.getVar(ConfVars.HIVE_CONF_RESTRICTED_LIST);
    assertTrue(newRestrictList.contains(ConfVars.SCRATCH_DIR.varname));
    assertTrue(newRestrictList.contains(ConfVars.LOCAL_SCRATCH_DIR.varname));
    assertTrue(newRestrictList.contains(ConfVars.METASTORE_URIS.varname));

    // check if the old values are still there in HIVE_CONF_RESTRICTED_LIST
    assertTrue(newRestrictList.contains(ConfVars.HIVE_TEST_MODE_PREFIX.varname));

    // verify that the new configs are in effect
    verifyRestriction(ConfVars.HIVE_TEST_MODE_PREFIX.varname, "foo");
    verifyRestriction(ConfVars.HIVE_CONF_RESTRICTED_LIST.varname, "foo");
    verifyRestriction(ConfVars.LOCAL_SCRATCH_DIR.varname, "foo");
    verifyRestriction(ConfVars.METASTORE_URIS.varname, "foo");
  }

  private void verifyRestriction(String varName, String newVal) {
    try {
      conf.verifyAndSet(varName, newVal);
      fail("Setting config property " + varName + " should fail");
    } catch (IllegalArgumentException e) {
      // the verifyAndSet in this case is expected to fail with the IllegalArgumentException
    }
  }
}
