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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.anon.AnonStatementAnalyzer;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestActiveVersionWins extends BaseTest {

  private static final String POLICY =
      "tavw_pol_" + Long.toString(System.nanoTime(), 36);

  private static final String V1_DSL =
      """
      VERSION 'v1'
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  private static final String V2_DSL =
      """
      VERSION 'v2'
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE ipList
      """;

  private static Path v1File;
  private static Path v2File;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  @BeforeAll
  public void writePolicyFiles() throws IOException {
    v1File = Files.createTempFile(POLICY + "_v1_", ".erp");
    Files.write(v1File, V1_DSL.getBytes());
    v2File = Files.createTempFile(POLICY + "_v2_", ".erp");
    Files.write(v2File, V2_DSL.getBytes());
  }

  @Test
  @Order(1)
  public void loadActivateTwoVersions()
      throws CommandProcessorException, HiveException {
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + v1File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    final ErasurePolicyVersion afterV1 = hive.getActiveErasurePolicyVersion(POLICY);
    Assertions.assertNotNull(afterV1, "v1 must be ACTIVE after the first ACTIVATE");

    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + v2File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    final ErasurePolicyVersion afterV2 = hive.getActiveErasurePolicyVersion(POLICY);
    Assertions.assertNotNull(afterV2, "v2 must be ACTIVE after the second ACTIVATE");
    Assertions.assertNotEquals(afterV1.getVersionId(), afterV2.getVersionId(),
        "v2 must be a distinct version row from v1");
  }

  @Test
  @Order(2)
  public void activeVersionSourceReflectsV2() throws HiveException {
    final ErasurePolicyVersion active = hive.getActiveErasurePolicyVersion(POLICY);
    Assertions.assertNotNull(active, "policy must have an ACTIVE version");
    final String src = active.getSourceText();
    Assertions.assertNotNull(src,
        "the ACTIVE version must carry a stored SOURCE_TEXT (the single source of truth)");
    Assertions.assertTrue(src.contains("ipList"),
        "active SOURCE_TEXT must be v2's body (erases ipList); got: " + src);
    Assertions.assertFalse(src.contains("telephone"),
        "active SOURCE_TEXT must NOT be the frozen v1 body (erases telephone); got: " + src);
    Assertions.assertEquals("v2", active.getVersionLabel(),
        "the live version label must be v2");
  }

  @Test
  @Order(3)
  public void describeRendersV2() throws CommandProcessorException {
    final List<Object> rows = execute("DESCRIBE ERASURE POLICY " + POLICY);
    final String text = String.valueOf(rows);
    Assertions.assertTrue(text.contains("ipList"),
        "DESCRIBE must render v2's body (ipList); got: " + text);
    Assertions.assertFalse(text.contains("telephone"),
        "DESCRIBE must NOT render the frozen v1 body (telephone); got: " + text);
  }

  @Test
  @Order(4)
  public void composeActiveVersionJsonPicksV2() throws Exception {
    final QueryState qs = new QueryState.Builder().withHiveConf(conf).build();
    final AnonStatementAnalyzer analyzer = new AnonStatementAnalyzer(qs);
    final Method m = AnonStatementAnalyzer.class
        .getDeclaredMethod("composeActiveVersionJson", String.class);
    m.setAccessible(true);
    final Object resolved = m.invoke(analyzer, POLICY);
    Assertions.assertNotNull(resolved,
        "composeActiveVersionJson must reconstruct the active body from SOURCE_TEXT, "
            + "not return null (which would force the frozen POLICY_DOC fallback)");
    final String json = String.valueOf(resolved);
    Assertions.assertTrue(json.contains("ipList"),
        "the resolved ERASE/EXTRACT body must be v2 (path ipList); got: " + json);
    Assertions.assertFalse(json.contains("telephone"),
        "the resolved ERASE/EXTRACT body must NOT fall to the frozen v1 body "
            + "(telephone); got: " + json);
  }

  @Test
  @Order(5)
  public void cleanup() {
    try { execute("DROP DATA ERASURE POLICY IF EXISTS " + POLICY); }
    catch (CommandProcessorException ignored) {  }
  }
}
