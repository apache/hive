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

import org.apache.hive.hep.ErasurePolicyValidator.ValidationError;
import org.apache.hive.hep.ErasurePolicyValidator.ValidationResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Semantic-validation tests for {@link ErasurePolicyValidator}.
 * Policy fixtures live under {@code src/test/resources/policies/} and are
 * loaded via {@link PolicyTestResources}.
 */
public class TestErasurePolicyValidator {

  @Test
  public void wellFormedPolicyValidates() {
    ValidationResult r = validateResource("valid-well-formed.erp");
    assertTrue("expected valid, got " + r.getErrors(), r.isValid());
  }

  @Test
  public void emptyStatementIsRejected() {
    ValidationResult r = validateResource("invalid-empty-statement.erp");
    assertFalse(r.isValid());
    assertContains(r, "must declare at least one of ERASE, REPLACE, INSPECT, or FLAG");
  }

  @Test
  public void overlappingPathIsRejected() {
    ValidationResult r = validateResource("invalid-overlapping-path.erp");
    assertFalse(r.isValid());
    assertContains(r, "appears in both ERASE and REPLACE");
  }

  @Test
  public void mismatchedSchemaTypeIsRejected() {
    ValidationResult r = validateResource("invalid-mismatched-schema-type.erp");
    assertFalse(r.isValid());
    assertContains(r, "STRING literal but SCHEMA TYPE is declared INT");
  }

  @Test
  public void schemaIntLiteralMatchingIntDeclWorks() {
    ValidationResult r = validateResource("valid-int-schema.erp");
    assertTrue("expected valid, got " + r.getErrors(), r.isValid());
  }

  @Test
  public void multipleStatementsValidateIndependently() {
    // First FOR SCHEMA block is well-formed; second contains an overlapping path.
    ValidationResult r = validateResource("mixed-stmt-overlap.erp");
    assertFalse(r.isValid());
    assertEquals(1, r.getErrors().size());
    assertContains(r, "FOR SCHEMA 'b'");
  }

  @Test
  public void overlapHonoursPredicates() {
    // xs[0]:y and xs[1]:y are textually distinct paths - not an overlap.
    ValidationResult r = validateResource("valid-predicates-distinct-indices.erp");
    assertTrue("indexed paths to distinct elements are not overlapping: "
        + r.getErrors(), r.isValid());
  }

  @Test
  public void overlapDetectedOnPredicatedPaths() {
    // Same path including the predicate -- this is an overlap.
    ValidationResult r = validateResource("invalid-same-predicated-path.erp");
    assertFalse(r.isValid());
    assertContains(r, "appears in both ERASE and REPLACE");
  }

  /**
   * The {@code :*} terminal wildcard is accepted by both parser and semantic
   * validator. Tests the three forms exercised by the fixture: simple
   * sub-object (\texttt{profile:*}), \texttt{List<BaseMsg>} wildcard
   * (\texttt{addresses[*]:*}), and filtered selector
   * (\texttt{sessions[type='guest']:*}).
   */
  @Test
  public void wildcardTerminalIsValid() {
    ValidationResult r = validateResource("valid-wildcard-terminal.erp");
    assertTrue("expected valid, got " + r.getErrors(), r.isValid());
  }

  /**
   * An interior STAR ({@code profile:*:firstName}) is rejected by the
   * semantic validator with a focused error message; the parser itself
   * accepts the path so that the validator can pinpoint the offending
   * step rather than emitting a generic syntax error.
   */
  @Test
  public void wildcardInteriorIsRejected() {
    ValidationResult r = validateResource("invalid-wildcard-interior.erp");
    assertFalse(r.isValid());
    assertContains(r, "wildcard '*' step is permitted only as the final step");
  }

  @Test
  public void syntaxErrorsShortCircuitSemanticChecks() {
    ValidationResult r = validateResource("unknown-keyword.erp");
    assertFalse(r.isValid());
    // No semantic errors should fire when parsing failed; only the syntax
    // error from the unknown keyword.
    for (ValidationError e : r.getErrors()) {
      assertFalse("unexpected semantic error after syntax failure: " + e,
          e.message.contains("must declare"));
    }
  }

  // ---------------------------------------------------------------------------

  private static ValidationResult validateResource(String name) {
    return ErasurePolicyValidator.validate(PolicyTestResources.load(name));
  }

  private static void assertContains(ValidationResult r, String needle) {
    for (ValidationError e : r.getErrors()) {
      if (e.message.contains(needle)) {
        return;
      }
    }
    throw new AssertionError("expected an error containing '" + needle
        + "', got " + r.getErrors());
  }
}
