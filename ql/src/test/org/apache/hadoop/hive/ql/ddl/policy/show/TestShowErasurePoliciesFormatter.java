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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SHOW ERASURE POLICIES emits one tab-separated row per policy. The earlier
 * listing wrote the raw multi-line {@code .erp} source as a column value, whose
 * embedded newlines split every source line into a bogus extra row (the policy
 * text landed in the name column and the second column showed null). These tests
 * pin the fix: every value is collapsed to a single line, so each policy is
 * exactly one three-column row.
 */
public class TestShowErasurePoliciesFormatter {

  private String render(List<ErasurePolicySummary> rows) throws HiveException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    new ShowErasurePoliciesFormatter.TextShowErasurePoliciesFormatter().showErasurePolicies(out, rows);
    return new String(bos.toByteArray(), StandardCharsets.UTF_8);
  }

  private static long tabs(String line) {
    return line.chars().filter(c -> c == '\t').count();
  }

  @Test
  void singleLineCollapsesTabsAndNewlines() {
    assertEquals("", ShowErasurePoliciesFormatter.singleLine(null));
    assertEquals("a b c", ShowErasurePoliciesFormatter.singleLine("a\nb\tc"));
    assertEquals("v1(ACTIVE)", ShowErasurePoliciesFormatter.singleLine("  v1(ACTIVE)\n"));
  }

  @Test
  void oneRowPerPolicyWithThreeColumns() throws Exception {
    List<ErasurePolicySummary> rows = Arrays.asList(
        new ErasurePolicySummary("cov_pii", "-", "v1(INACTIVE)"),
        new ErasurePolicySummary("billing", "v3", "v1(SUPERSEDED), v2(SUPERSEDED), v3(ACTIVE)"));

    String[] lines = render(rows).split("\n", -1);

    assertEquals(3, lines.length, "two data rows plus the trailing empty element");
    assertEquals("", lines[2]);
    assertEquals(2, tabs(lines[0]), "exactly three tab-separated columns");
    assertEquals(2, tabs(lines[1]), "exactly three tab-separated columns");
    assertTrue(lines[0].startsWith("cov_pii\t-\tv1(INACTIVE)"), lines[0]);
    assertTrue(lines[1].startsWith("billing\tv3\tv1(SUPERSEDED), v2(SUPERSEDED), v3(ACTIVE)"), lines[1]);
  }

  @Test
  void embeddedNewlineDoesNotSplitIntoExtraRows() throws Exception {
    // Reproduces the original symptom: a multi-line value must not create new rows.
    List<ErasurePolicySummary> rows = Arrays.asList(
        new ErasurePolicySummary("p", "-", "IDENTITY userId TYPE INT\nSCHEMA TYPE INT\nERASE telephone"));

    String[] lines = render(rows).split("\n", -1);

    assertEquals(2, lines.length, "one data row plus trailing empty, despite embedded newlines");
    assertEquals(2, tabs(lines[0]), "still exactly three columns");
    assertTrue(lines[0].contains("IDENTITY userId TYPE INT SCHEMA TYPE INT ERASE telephone"), lines[0]);
  }
}
