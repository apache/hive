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

package org.apache.hive.hep;

import org.apache.hive.hep.ParsedPolicy.ActionKind;
import org.apache.hive.hep.PolicyConflictDetector.ConflictClass;
import org.apache.hive.hep.PolicyConflictDetector.Report;
import org.apache.hive.hep.PolicyConflictDetector.ResolutionMode;
import org.apache.hive.hep.PolicyConflictDetector.ResolvedRule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the §5.6 conflict detector. Each test loads two .erp fixtures
 * from {@code src/test/resources/policies/}, hands them to the detector,
 * and checks the produced report.
 */
public class TestPolicyConflictDetector {

  // ---------------------------------------------------------------------------
  // C1 — action conflict (ERASE vs REPLACE on the same field).
  // ---------------------------------------------------------------------------

  @Test
  public void c1ActionConflictDetected() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-c1-erase-email.erp",
        "conflict-c1-replace-email.erp");
    assertEquals(1, r.conflicts.size());
    assertEquals(ConflictClass.C1_ACTION, r.conflicts.get(0).kind);
  }

  @Test
  public void c1StrictestEscalatesToErase() {
    Report r = analyse(
        ResolutionMode.STRICTEST,
        "conflict-c1-erase-email.erp",
        "conflict-c1-replace-email.erp");
    assertEquals(1, r.resolvedRules.size());
    ResolvedRule rr = r.resolvedRules.get(0);
    assertEquals("email", rr.rule.fieldPath);
    assertEquals(ActionKind.ERASE, rr.rule.action);
    assertTrue(rr.resolutionNote != null && rr.resolutionNote.contains("C1"));
  }

  // ---------------------------------------------------------------------------
  // C2 — value conflict (two REPLACEs with different literal values).
  // ---------------------------------------------------------------------------

  @Test
  public void c2ValueConflictDetected() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-c2-replace-unknown.erp",
        "conflict-c2-replace-eu.erp");
    assertEquals(1, r.conflicts.size());
    assertEquals(ConflictClass.C2_VALUE, r.conflicts.get(0).kind);
  }

  @Test
  public void c2StrictestEscalatesToErase() {
    Report r = analyse(
        ResolutionMode.STRICTEST,
        "conflict-c2-replace-unknown.erp",
        "conflict-c2-replace-eu.erp");
    assertEquals(1, r.resolvedRules.size());
    assertEquals(ActionKind.ERASE, r.resolvedRules.get(0).rule.action);
    assertTrue(r.resolvedRules.get(0).resolutionNote.contains("C2"));
  }

  // ---------------------------------------------------------------------------
  // C3 — path-prefix conflict (outer path is a strict prefix of inner).
  // ---------------------------------------------------------------------------

  @Test
  public void c3PathPrefixConflictDetected() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-c3-erase-profile.erp",
        "conflict-c3-replace-profile-first.erp");
    assertEquals(1, r.conflicts.size());
    assertEquals(ConflictClass.C3_PATH_PREFIX, r.conflicts.get(0).kind);
  }

  @Test
  public void c3StrictestDropsInner() {
    Report r = analyse(
        ResolutionMode.STRICTEST,
        "conflict-c3-erase-profile.erp",
        "conflict-c3-replace-profile-first.erp");
    // The inner profile:firstName rule is subsumed by the outer wildcard
    // ERASE profile:*, which covers every descendant of the profile
    // sub-object under the §4.2 wildcard-terminal semantics.
    assertEquals(1, r.resolvedRules.size());
    assertEquals("profile:*", r.resolvedRules.get(0).rule.fieldPath);
    assertEquals(ActionKind.ERASE, r.resolvedRules.get(0).rule.action);
  }

  /**
   * Legacy backward-compatibility: a bare sub-object path (without the
   * {@code :*} wildcard) is still detected as a C3 covering rule against a
   * deeper leaf path. The §4.2 anonymisability check is expected to reject
   * such bare-prefix policies before they reach the detector, but the
   * detector treats them as covering for defence in depth.
   */
  @Test
  public void c3LegacyBarePrefixStillDetectedForCompatibility() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-c3-erase-legacy-profile.erp",
        "conflict-c3-replace-profile-first.erp");
    assertEquals(1, r.conflicts.size());
    assertEquals(ConflictClass.C3_PATH_PREFIX, r.conflicts.get(0).kind);
  }

  // ---------------------------------------------------------------------------
  // C4 — cross-schema rules commute (no conflict expected).
  // ---------------------------------------------------------------------------

  @Test
  public void crossSchemaRulesDoNotConflict() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-cross-schema-erase-m1.erp",
        "conflict-cross-schema-replace-m2.erp");
    assertTrue(r.conflicts.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Non-conflict cases.
  // ---------------------------------------------------------------------------

  @Test
  public void identicalRulesProduceNoConflict() {
    // Same fixture loaded twice -- a's and b's contents are identical.
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-c2-replace-unknown.erp",
        "conflict-c2-replace-unknown.erp");
    assertTrue(r.conflicts.isEmpty());
  }

  @Test
  public void distinctIndexedPathsDoNotConflict() {
    Report r = analyse(
        ResolutionMode.EXPLICIT,
        "conflict-distinct-indices-erase-0.erp",
        "conflict-distinct-indices-replace-1.erp");
    assertTrue("distinct indexed paths target distinct elements", r.conflicts.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // Edge cases.
  // ---------------------------------------------------------------------------

  @Test
  public void emptyInputProducesEmptyReport() {
    Report r = PolicyConflictDetector.analyse(new LinkedHashMap<>(), ResolutionMode.EXPLICIT);
    assertFalse(r.hasConflicts());
    assertTrue(r.resolvedRules.isEmpty());
  }

  @Test
  public void singlePolicyHasNoConflictsButHasResolvedRules() {
    Map<String, ParsedPolicy> one = new LinkedHashMap<>();
    one.put("solo", loadPolicy("conflict-single-mixed.erp"));

    Report r = PolicyConflictDetector.analyse(one, ResolutionMode.STRICTEST);
    assertTrue(r.conflicts.isEmpty());
    assertEquals(2, r.resolvedRules.size());
  }

  // ---------------------------------------------------------------------------

  private static ParsedPolicy loadPolicy(String name) {
    return ErasurePolicyValidator.parse(PolicyTestResources.load(name));
  }

  /** Load two policies from fixtures and analyse them under the given mode. */
  private static Report analyse(ResolutionMode mode, String fixtureA, String fixtureB) {
    Map<String, ParsedPolicy> m = new LinkedHashMap<>();
    m.put("policy_a", loadPolicy(fixtureA));
    m.put("policy_b", loadPolicy(fixtureB));
    return PolicyConflictDetector.analyse(m, mode);
  }
}
