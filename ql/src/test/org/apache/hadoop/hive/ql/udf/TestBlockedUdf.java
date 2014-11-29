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
 * distributed under the License is distributed on an "import org.apache.hadoop.hive.ql.exec.FunctionInfo;
AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf;

import static org.junit.Assert.*;

import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBlockedUdf {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    FunctionRegistry.setupPermissionsForBuiltinUDFs("", "");
  }

  /**
   * Verify that UDF in the whitelist can be access
   * @throws Exception
   */
  @Test
  public void testDefaultWhiteList() throws Exception {
    assertEquals("", new HiveConf().getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_WHITELIST));
    assertEquals("", new HiveConf().getVar(ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST));
    FunctionRegistry.setupPermissionsForBuiltinUDFs("", "");
    assertEquals("substr", FunctionRegistry.getFunctionInfo("substr").getDisplayName());
  }

  /**
   * Verify that UDF in the whitelist can be access
   * @throws Exception
   */
  @Test
  public void testUdfInWhiteList() throws Exception {
    Set<String> funcNames = FunctionRegistry.getFunctionNames();
    funcNames.remove("reflect");
    FunctionRegistry.setupPermissionsForBuiltinUDFs(funcNames.toString(), "");
    assertEquals("substr", FunctionRegistry.getFunctionInfo("substr").getDisplayName());
  }

  /**
   * Verify that UDF not in whitelist can't be accessed
   * @throws Exception
   */
  @Test (expected=SemanticException.class)
  public void testUdfNotInWhiteList() throws Exception {
    Set<String> funcNames = FunctionRegistry.getFunctionNames();
    funcNames.remove("reflect");
    FunctionRegistry.setupPermissionsForBuiltinUDFs(funcNames.toString(), "");
    assertEquals("reflect", FunctionRegistry.getFunctionInfo("reflect").getDisplayName());
  }

  /**
   * Verify that UDF in blacklist can't be accessed
   * @throws Exception
   */
  @Test (expected=SemanticException.class)
  public void testUdfInBlackList() throws Exception {
    FunctionRegistry.setupPermissionsForBuiltinUDFs("", "reflect");
    assertEquals("reflect", FunctionRegistry.getFunctionInfo("reflect").getDisplayName());
  }

  /**
   * Verify that UDF in whitelist and blacklist can't be accessed
   * @throws Exception
   */
  @Test (expected=SemanticException.class)
  public void testUdfInBlackAndWhiteList() throws Exception {
    FunctionRegistry.setupPermissionsForBuiltinUDFs("reflect", "reflect");
    assertEquals("reflect", FunctionRegistry.getFunctionInfo("reflect").getDisplayName());
  }

  /**
   * Test malformatted udf  list setting
   */
  @Test (expected=SemanticException.class)
  public void testMalformattedListProperty() throws Exception {
    FunctionRegistry.setupPermissionsForBuiltinUDFs(",,", " ,reflect,");
    assertEquals("reflect", FunctionRegistry.getFunctionInfo("reflect").getDisplayName());
  }

}
