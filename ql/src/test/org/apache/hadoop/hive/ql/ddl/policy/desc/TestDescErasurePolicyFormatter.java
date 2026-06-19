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

package org.apache.hadoop.hive.ql.ddl.policy.desc;

import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.metastore.api.PolicyLiteralKind;
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DESCRIBE ERASURE POLICY renders as a single text column: a governance header
 * then the policy source, one line per row. The earlier listing crammed the
 * multi-line source into a two-column {@code name,doc} schema, whose newlines
 * split every source line into a bogus row with NULL in the second column. These
 * tests pin the single-column rendering: no NULLs, source preserved line-by-line.
 */
public class TestDescErasurePolicyFormatter {

  private ErasurePolicy policy(String name) {
    ErasurePolicy p = new ErasurePolicy();
    p.setPolicyName(name);
    return p;
  }

  private ErasurePolicyVersion version(long id, String label, PolicyVersionStatus status) {
    ErasurePolicyVersion v = new ErasurePolicyVersion();
    v.setVersionId(id);
    v.setVersionLabel(label);
    v.setStatus(status);
    return v;
  }

  /** An ACTIVE version carrying the given .erp source body. */
  private ErasurePolicyVersion activeWithSource(long id, String label, String source) {
    ErasurePolicyVersion v = version(id, label, PolicyVersionStatus.ACTIVE);
    v.setSourceText(source);
    return v;
  }

  @Test
  void headerThenSourceNoNulls() {
    ErasurePolicy p = policy("smoke_pii");
    List<ErasurePolicyVersion> versions = Arrays.asList(activeWithSource(1, "v1",
        "IDENTITY userId TYPE INT\nSCHEMA TYPE INT\n\nFOR SCHEMA 4\n    ERASE telephone, country\n"));

    List<String> lines = DescErasurePolicyFormatter.renderLines(p, versions, null, false);

    assertTrue(lines.stream().noneMatch(l -> l == null), "no rendered line is null");
    assertEquals("Policy:    smoke_pii", lines.get(0));
    assertEquals("Active:    v1", lines.get(1));
    assertEquals("Versions:  v1(ACTIVE)", lines.get(2));
    assertEquals("", lines.get(3));
    assertTrue(lines.contains("IDENTITY userId TYPE INT"));
    assertTrue(lines.contains("    ERASE telephone, country"));
    // the trailing newline must not leave a dangling blank row
    assertEquals("    ERASE telephone, country", lines.get(lines.size() - 1));
  }

  @Test
  void noActiveVersionShowsNone() {
    ErasurePolicy p = policy("cov_pii");
    List<ErasurePolicyVersion> versions = Arrays.asList(version(1, "v1", PolicyVersionStatus.INACTIVE));
    List<String> lines = DescErasurePolicyFormatter.renderLines(p, versions, null, false);
    assertEquals("Active:    (none)", lines.get(1));
    assertEquals("Versions:  v1(INACTIVE)", lines.get(2));
  }

  @Test
  void tabsInSourceNeutralised() {
    ErasurePolicy p = policy("p");
    List<ErasurePolicyVersion> versions =
        Arrays.asList(activeWithSource(1, "v1", "FOR SCHEMA 4\n\tERASE telephone"));
    List<String> lines = DescErasurePolicyFormatter.renderLines(p, versions, null, false);
    assertTrue(lines.stream().noneMatch(l -> l.contains("\t")), "no tabs in any rendered line");
    assertTrue(lines.contains(" ERASE telephone"));
  }

  @Test
  void extendedAddsProvenance() {
    ErasurePolicy p = policy("smoke_pii");
    ErasurePolicyVersion v = version(1, "v1", PolicyVersionStatus.ACTIVE);
    v.setIdentityFieldName("userId");
    v.setIdentityFieldType(PolicyLiteralKind.INT);
    v.setSchemaType(PolicyLiteralKind.INT);
    v.setValidatedBy("alice");
    v.setActivatedBy("bob");
    v.setSourceChecksum("deadbeef");

    List<String> lines = DescErasurePolicyFormatter.renderLines(p, Arrays.asList(v), null, true);

    assertTrue(lines.contains("Identity:  userId (INT)"), lines.toString());
    assertTrue(lines.contains("Validated: alice"));
    assertTrue(lines.contains("Activated: bob"));
    assertTrue(lines.contains("Source SHA-256: deadbeef"));
  }

  /**
   * Per-version body selection: v1 is LOADed first (now SUPERSEDED), v2 LOADed
   * after and ACTIVE. Plain DESCRIBE must render the ACTIVE v2's body, not v1's
   * (the policy-header doc still holds v1's source). VERSION 'v1' renders v1's.
   */
  @Test
  void activeVersionBodyRendered() {
    ErasurePolicy p = policy("multi");
    ErasurePolicyVersion v1 = version(1, "v1", PolicyVersionStatus.SUPERSEDED);
    v1.setSourceText("IDENTITY userId TYPE INT\nSCHEMA TYPE INT\n\nFOR SCHEMA 3\n    ERASE telephone\n");
    ErasurePolicyVersion v2 = version(2, "v2", PolicyVersionStatus.ACTIVE);
    v2.setSourceText("IDENTITY userId TYPE INT\nSCHEMA TYPE INT\n\nFOR SCHEMA 3\n    ERASE telephone, country\n");
    List<ErasurePolicyVersion> vs = Arrays.asList(v1, v2);

    // Plain DESCRIBE renders the ACTIVE version (v2).
    List<String> active = DescErasurePolicyFormatter.renderLines(p, vs, null, false);
    assertEquals("Active:    v2", active.get(1));
    assertTrue(active.contains("    ERASE telephone, country"), "renders active v2 body: " + active);
    assertTrue(active.stream().noneMatch(l -> l.equals("    ERASE telephone")),
        "must NOT render v1's body for plain DESCRIBE: " + active);

    // DESCRIBE ... VERSION 'v1' renders v1's body.
    List<String> sel = DescErasurePolicyFormatter.renderLines(p, vs, "v1", false);
    assertTrue(sel.contains("Showing:   v1 (SUPERSEDED)"), sel.toString());
    assertTrue(sel.contains("    ERASE telephone"), "renders selected v1 body: " + sel);
    assertTrue(sel.stream().noneMatch(l -> l.equals("    ERASE telephone, country")),
        "VERSION v1 must not render v2's body: " + sel);

    // Unknown version is reported, not silently wrong.
    List<String> bad = DescErasurePolicyFormatter.renderLines(p, vs, "v9", false);
    assertTrue(bad.stream().anyMatch(l -> l.contains("no version 'v9'")), bad.toString());
  }
}
