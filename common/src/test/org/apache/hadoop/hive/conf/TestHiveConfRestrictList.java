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
        ConfVars.HIVETESTMODEPREFIX.varname);
    conf = new HiveConf();
  }

  /**
   * Test that configs in restrict list can't be changed
   * @throws Exception
   */
  @Test
  public void testRestriction() throws Exception {
    verifyRestriction(ConfVars.HIVETESTMODEPREFIX.varname, "foo");
    conf.verifyAndSet(ConfVars.HIVE_AM_SPLIT_GENERATION.varname, "false");
  }

  /**
   * Test that configs in restrict list can't be changed
   * @throws Exception
   */
  @Test
  public void testMultipleRestrictions() throws Exception {
    verifyRestriction(ConfVars.HIVETESTMODEPREFIX.varname, "foo");
    verifyRestriction(ConfVars.HIVE_IN_TEST.varname, "true");
  }

  /**
   * Test that restrict list config itselft can't be changed
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
    String appendListStr = ConfVars.SCRATCHDIR.varname + "," +
        ConfVars.LOCALSCRATCHDIR.varname + "," +
        ConfVars.METASTOREURIS.varname;

    conf.addToRestrictList(appendListStr);
    // check if the new configs are added to HIVE_CONF_RESTRICTED_LIST
    String newRestrictList = conf.getVar(ConfVars.HIVE_CONF_RESTRICTED_LIST);
    assertTrue(newRestrictList.contains(ConfVars.SCRATCHDIR.varname));
    assertTrue(newRestrictList.contains(ConfVars.LOCALSCRATCHDIR.varname));
    assertTrue(newRestrictList.contains(ConfVars.METASTOREURIS.varname));

    // check if the old values are still there in HIVE_CONF_RESTRICTED_LIST
    assertTrue(newRestrictList.contains(ConfVars.HIVETESTMODEPREFIX.varname));

    // verify that the new configs are in effect
    verifyRestriction(ConfVars.HIVETESTMODEPREFIX.varname, "foo");
    verifyRestriction(ConfVars.HIVE_CONF_RESTRICTED_LIST.varname, "foo");
    verifyRestriction(ConfVars.LOCALSCRATCHDIR.varname, "foo");
    verifyRestriction(ConfVars.METASTOREURIS.varname, "foo");
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
