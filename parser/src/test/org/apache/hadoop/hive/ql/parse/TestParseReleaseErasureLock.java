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
 * AST-shape tests for {@code RELEASE ERASURE LOCK ON TABLE t [FORCE]
 * [WITH REASON '<text>']} and the related {@code SHOW ERASURE LOCKS}.
 *
 * <p>The grammar (in AnonParser.g and ShowStatementParser.g) treats
 * FORCE and WITH REASON as independently optional. The semantic
 * constraint that FORCE requires WITH REASON is enforced by
 * {@code AnonStatementAnalyzer.analyzeReleaseErasureLock} on the AST
 * shape exercised below: the analyzer rejects the command whenever a
 * TOK_FORCE_RELEASE child is present but no TOK_RELEASE_REASON sibling
 * is, exactly the shape produced by
 * {@link #releaseForceWithoutReasonProducesForceWithoutReasonChild()}.
 */
public class TestParseReleaseErasureLock {
  private final ParseDriver parseDriver = new ParseDriver();

  @Test
  public void loadErasurePolicyProducesLepToken() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "LOAD ERASURE POLICY player_pii FROM '/policies/player_pii.erp'").getTree();
    Assert.assertTrue("LOAD ERASURE POLICY ... FROM '...' should produce TOK_LEP, got: "
        + tree.toStringTree(),
        tree.toStringTree().contains("tok_lep"));
  }

  @Test
  public void showErasureLocksProducesGlobalToken() throws Exception {
    final ASTNode tree = parseDriver.parse("SHOW ERASURE LOCKS").getTree();
    Assert.assertTrue("SHOW ERASURE LOCKS should produce TOK_SHOW_ERASURE_LOCKS, got: "
        + tree.toStringTree(),
        tree.toStringTree().contains("tok_show_erasure_locks"));
  }

  @Test
  public void softReleaseWithoutReason() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "RELEASE ERASURE LOCK ON TABLE user_info").getTree();
    final ASTNode rel = findReleaseRoot(tree);
    Assert.assertNotNull("parser must produce TOK_RELEASE_ERASURE_LOCK", rel);
    Assert.assertFalse("soft release without WITH REASON must not carry TOK_FORCE_RELEASE",
        hasChild(rel, HiveParser.TOK_FORCE_RELEASE));
    Assert.assertFalse("soft release without WITH REASON must not carry TOK_RELEASE_REASON",
        hasChild(rel, HiveParser.TOK_RELEASE_REASON));
  }

  @Test
  public void softReleaseWithReason() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "RELEASE ERASURE LOCK ON TABLE user_info WITH REASON 'ops ticket 4711'")
        .getTree();
    final ASTNode rel = findReleaseRoot(tree);
    Assert.assertNotNull(rel);
    Assert.assertFalse(hasChild(rel, HiveParser.TOK_FORCE_RELEASE));
    final ASTNode reason = findChild(rel, HiveParser.TOK_RELEASE_REASON);
    Assert.assertNotNull("WITH REASON must produce TOK_RELEASE_REASON", reason);
    Assert.assertEquals("'ops ticket 4711'", reason.getChild(0).getText());
  }

  @Test
  public void forceReleaseWithReason() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "RELEASE ERASURE LOCK ON TABLE user_info FORCE WITH REASON 'cluster restart'")
        .getTree();
    final ASTNode rel = findReleaseRoot(tree);
    Assert.assertNotNull(rel);
    Assert.assertTrue("FORCE must produce TOK_FORCE_RELEASE",
        hasChild(rel, HiveParser.TOK_FORCE_RELEASE));
    final ASTNode reason = findChild(rel, HiveParser.TOK_RELEASE_REASON);
    Assert.assertNotNull("WITH REASON must produce TOK_RELEASE_REASON", reason);
    Assert.assertEquals("'cluster restart'", reason.getChild(0).getText());
  }

  /**
   * The parser accepts {@code FORCE} without {@code WITH REASON}: both
   * tokens are independently optional at the grammar level. The AST
   * carries TOK_FORCE_RELEASE without a sibling TOK_RELEASE_REASON,
   * which is the exact shape the analyzer's force-vs-reason check
   * refuses (see AnonStatementAnalyzer.analyzeReleaseErasureLock).
   *
   * This test fixes the contract between parser and analyzer: if the
   * AST shape produced here changes, the analyzer's rejection logic
   * needs to be revisited at the same time.
   */
  @Test
  public void releaseForceWithoutReasonProducesForceWithoutReasonChild() throws Exception {
    final ASTNode tree = parseDriver.parse(
        "RELEASE ERASURE LOCK ON TABLE user_info FORCE").getTree();
    final ASTNode rel = findReleaseRoot(tree);
    Assert.assertNotNull("parser must accept FORCE without WITH REASON at the grammar "
        + "level (analyzer rejects later)", rel);
    Assert.assertTrue("FORCE must produce TOK_FORCE_RELEASE",
        hasChild(rel, HiveParser.TOK_FORCE_RELEASE));
    Assert.assertFalse("FORCE without WITH REASON must NOT produce TOK_RELEASE_REASON; "
        + "the analyzer detects this missing-child shape and refuses the command",
        hasChild(rel, HiveParser.TOK_RELEASE_REASON));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static ASTNode findReleaseRoot(ASTNode n) {
    if (n.getType() == HiveParser.TOK_RELEASE_ERASURE_LOCK) {
      return n;
    }
    for (int i = 0; i < n.getChildCount(); i++) {
      final ASTNode r = findReleaseRoot((ASTNode) n.getChild(i));
      if (r != null) {
        return r;
      }
    }
    return null;
  }

  private static boolean hasChild(ASTNode parent, int tokenType) {
    return findChild(parent, tokenType) != null;
  }

  private static ASTNode findChild(ASTNode parent, int tokenType) {
    for (int i = 0; i < parent.getChildCount(); i++) {
      final ASTNode c = (ASTNode) parent.getChild(i);
      if (c.getType() == tokenType) {
        return c;
      }
    }
    return null;
  }
}
