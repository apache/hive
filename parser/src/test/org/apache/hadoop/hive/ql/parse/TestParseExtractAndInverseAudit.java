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
package org.apache.hadoop.hive.ql.parse;

import org.junit.Assert;
import org.junit.Test;

/**
 * AST-shape tests for the §4.8 surface added in {@code AnonParser.g}:
 *
 * <ul>
 *   <li>{@code EXTRACT FROM TABLE t COLUMN c FOR IDENTITY VALUES (...)}
 *       — Article 15 / Article 20 subject-access read. Mirrors
 *       {@code ERASE FROM TABLE} in shape but takes an explicit
 *       {@code COLUMN} so the binding lookup is unambiguous and produces
 *       {@code TOK_EXTRACT_FROM_TABLE} with three children:
 *       {@code (tableName, columnName, TOK_AT_PL)}.</li>
 *   <li>{@code AUDIT BY IDENTITY VALUES (...)} — Article 33
 *       subject-keyed inverse audit. Produces {@code TOK_AUDIT_BY_IDENTITY}
 *       with one child: {@code TOK_AT_PL}.</li>
 * </ul>
 *
 * <p>Parser-level tests (as opposed to {@code BaseTest}-based end-to-end
 * tests) so that the §4.8 grammar contract is locked in without
 * depending on the in-process driver, embedded Derby Metastore, or the
 * stats-annotation pipeline downstream.
 */
public class TestParseExtractAndInverseAudit {
  private final ParseDriver parseDriver = new ParseDriver();

  // ---------------------------------------------------------------- EXTRACT

  @Test
  public void extractFromTableProducesExtractFromTableToken() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "EXTRACT FROM TABLE user_info COLUMN body FOR IDENTITY VALUES (42)").getTree();
    Assert.assertTrue(
        "EXTRACT FROM TABLE ... should produce TOK_EXTRACT_FROM_TABLE, got: "
            + tree.toStringTree(),
        tree.toStringTree().contains("tok_extract_from_table"));
  }

  @Test
  public void extractFromTableCarriesColumnAndIdentityList() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "EXTRACT FROM TABLE user_info COLUMN body FOR IDENTITY VALUES (42, 99, 'alice')")
        .getTree();
    final ASTNode extract = findFirst(tree, HiveParser.TOK_EXTRACT_FROM_TABLE);
    Assert.assertNotNull("parser must produce TOK_EXTRACT_FROM_TABLE", extract);

    // Three children: (tableName, columnName, TOK_AT_PL) — same shape
    // ERASE produces, with the addition of an explicit COLUMN child
    // between the table name and the identity-value list.
    Assert.assertEquals(
        "TOK_EXTRACT_FROM_TABLE must carry exactly three children "
            + "(tableName, columnName, TOK_AT_PL)",
        3, extract.getChildCount());

    // Identity-value list is the third child and is a TOK_AT_PL with
    // one child per supplied value — same shape ERASE uses.
    final ASTNode atpl = (ASTNode) extract.getChild(2);
    Assert.assertEquals("third child must be TOK_AT_PL",
        HiveParser.TOK_AT_PL, atpl.getType());
    Assert.assertEquals("identity-value list size must equal the number of supplied values",
        3, atpl.getChildCount());
  }

  @Test
  public void extractFromTableRequiresIdentityValueList() {
    // Empty parentheses must fail at parse time: the pidList rule
    // requires at least one pidValue token.
    boolean threw = false;
    try {
      parseDriver.parse(
          "EXTRACT FROM TABLE user_info COLUMN body FOR IDENTITY VALUES ()").getTree();
    } catch (Exception expected) {
      threw = true;
    }
    Assert.assertTrue("EXTRACT FROM TABLE with an empty identity list must fail to parse",
        threw);
  }

  @Test
  public void extractFromTableRejectsMissingColumnKeyword() {
    // The COLUMN keyword is mandatory: removing it must produce a
    // parse error. This pins the §4.8 contract that an EXTRACT names
    // the bound column explicitly rather than relying on legacy
    // table-only binding (the ERASE shape).
    boolean threw = false;
    try {
      parseDriver.parse(
          "EXTRACT FROM TABLE user_info FOR IDENTITY VALUES (42)").getTree();
    } catch (Exception expected) {
      threw = true;
    }
    Assert.assertTrue("EXTRACT FROM TABLE without COLUMN must fail to parse", threw);
  }

  // ------------------------------------------------- AUDIT BY IDENTITY VALUES

  @Test
  public void auditByIdentityProducesAuditByIdentityToken() throws Exception {
    final ASTNode tree = parseDriver.parse("AUDIT BY IDENTITY VALUES (42)").getTree();
    Assert.assertTrue(
        "AUDIT BY IDENTITY VALUES (...) should produce TOK_AUDIT_BY_IDENTITY, got: "
            + tree.toStringTree(),
        tree.toStringTree().contains("tok_audit_by_identity"));
  }

  @Test
  public void auditByIdentityCarriesIdentityList() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "AUDIT BY IDENTITY VALUES (1, 42, 'alice', 99)").getTree();
    final ASTNode audit = findFirst(tree, HiveParser.TOK_AUDIT_BY_IDENTITY);
    Assert.assertNotNull("parser must produce TOK_AUDIT_BY_IDENTITY", audit);

    // Single child: TOK_AT_PL with one entry per supplied value.
    Assert.assertEquals("TOK_AUDIT_BY_IDENTITY must carry exactly one child (TOK_AT_PL)",
        1, audit.getChildCount());
    final ASTNode atpl = (ASTNode) audit.getChild(0);
    Assert.assertEquals("only child must be TOK_AT_PL",
        HiveParser.TOK_AT_PL, atpl.getType());
    Assert.assertEquals(4, atpl.getChildCount());
  }

  @Test
  public void auditByIdentityDoesNotCollideWithAuditErasureExecutions() throws Exception {
    // AUDIT ERASURE EXECUTIONS is a different verb in the same family
    // and shares the AUDIT prefix. The parser must keep the two paths
    // distinct: AUDIT BY ... produces TOK_AUDIT_BY_IDENTITY whereas
    // AUDIT ERASURE EXECUTIONS produces TOK_AUDIT_EXEC.
    final ASTNode byIdentity = parseDriver.parse(
        "AUDIT BY IDENTITY VALUES (42)").getTree();
    Assert.assertNotNull(findFirst(byIdentity, HiveParser.TOK_AUDIT_BY_IDENTITY));
    Assert.assertNull("AUDIT BY IDENTITY must not also emit TOK_AUDIT_EXEC",
        findFirst(byIdentity, HiveParser.TOK_AUDIT_EXEC));

    final ASTNode auditExec = parseDriver.parse(
        "AUDIT ERASURE EXECUTIONS ON TABLE user_info").getTree();
    Assert.assertNotNull(findFirst(auditExec, HiveParser.TOK_AUDIT_EXEC));
    Assert.assertNull("AUDIT ERASURE EXECUTIONS must not also emit TOK_AUDIT_BY_IDENTITY",
        findFirst(auditExec, HiveParser.TOK_AUDIT_BY_IDENTITY));
  }

  // --------------------------------------------- backward compatibility guard

  @Test
  public void existingLoadErasurePolicyShapeIsUnchanged() throws Exception {
    // Regression guard. Adding the §4.8 productions must not perturb
    // the existing §4.4 lifecycle grammar: LOAD ERASURE POLICY still
    // emits TOK_LEP, ACTIVATE still emits TOK_AEP, etc.
    final ASTNode loadTree = parseDriver.parse(
        "LOAD ERASURE POLICY player_pii FROM '/policies/player_pii.erp'").getTree();
    Assert.assertNotNull("LOAD ERASURE POLICY must still emit TOK_LEP",
        findFirst(loadTree, HiveParser.TOK_LEP));
    Assert.assertNull("LOAD ERASURE POLICY must not also emit TOK_EXTRACT_FROM_TABLE",
        findFirst(loadTree, HiveParser.TOK_EXTRACT_FROM_TABLE));
  }

  @Test
  public void existingEraseFromTableShapeIsUnchanged() throws Exception {
    final ASTNode eraseTree = parseDriver.parse(
        "ERASE FROM TABLE user_info FOR IDENTITY VALUES (42)").getTree();
    final ASTNode erase = findFirst(eraseTree, HiveParser.TOK_AT);
    Assert.assertNotNull("ERASE FROM TABLE must still emit TOK_AT", erase);
    // ERASE produces (tableName, TOK_AT_PL) — two children, no COLUMN.
    Assert.assertEquals("ERASE TOK_AT shape must be (tableName, TOK_AT_PL); 2 children",
        2, erase.getChildCount());
  }

  // ----------------------------------------------------------------- helpers

  private static ASTNode findFirst(ASTNode n, int tokenType) {
    if (n.getType() == tokenType) {
      return n;
    }
    for (int i = 0; i < n.getChildCount(); i++) {
      final ASTNode r = findFirst((ASTNode) n.getChild(i), tokenType);
      if (r != null) {
        return r;
      }
    }
    return null;
  }
}
