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

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Smoke tests for the generated ErasurePolicy parser.
 * Policy fixtures live under {@code src/test/resources/policies/} and are
 * loaded via {@link PolicyTestResources}. No semantic validation is performed
 * here; that is the responsibility of {@link ErasurePolicyValidator}.
 */
public class TestErasurePolicyParser {

  @Test
  public void exampleParsesWithoutSyntaxErrors() {
    ParseResult result = parse(PolicyTestResources.load("example.erp"));
    assertTrue(
        "expected no syntax errors but got " + result.errors,
        result.errors.isEmpty());
  }

  @Test
  public void parsesOptionalLplHeaders() {
    String src =
        "VERSION v3\n"
        + "PURPOSE 'AML retention'\n"
        + "LEGAL BASIS 'legal-obligation'\n"
        + "IDENTITY userId TYPE INT\n"
        + "SCHEMA TYPE INT\n"
        + "FOR SCHEMA 4\n"
        + "    ERASE telephone, country\n";
    ParsedPolicy p = ErasurePolicyValidator.parse(src);
    assertEquals("v3", p.getVersion());
    assertEquals("AML retention", p.getPurpose());
    assertEquals("legal-obligation", p.getLegalBasis());
    assertEquals(1, p.getStatements().size());
  }

  @Test
  public void lplHeadersAreOptional() {
    String src =
        "IDENTITY userId TYPE INT\n"
        + "SCHEMA TYPE INT\n"
        + "FOR SCHEMA 4\n"
        + "    ERASE telephone\n";
    ParsedPolicy p = ErasurePolicyValidator.parse(src);
    assertNull(p.getVersion());
    assertNull(p.getPurpose());
    assertNull(p.getLegalBasis());
  }

  @Test
  public void quotedVersionHeaderUnquotes() {
    String src =
        "VERSION '1.2.0'\n"
        + "IDENTITY userId TYPE INT\n"
        + "SCHEMA TYPE INT\n"
        + "FOR SCHEMA 4\n"
        + "    ERASE telephone\n";
    assertEquals("1.2.0", ErasurePolicyValidator.parse(src).getVersion());
  }


  @Test
  public void singleSchemaWithEraseOnly() {
    assertTrue(parse(PolicyTestResources.load("single-erase.erp")).errors.isEmpty());
  }

  @Test
  public void singleSchemaWithReplaceOnly() {
    assertTrue(parse(PolicyTestResources.load("single-replace.erp")).errors.isEmpty());
  }

  @Test
  public void predicateFormsParse() {
    assertTrue(parse(PolicyTestResources.load("predicate-forms.erp")).errors.isEmpty());
  }

  /**
   * The {@code :*} terminal wildcard is a valid pathStep alternative.
   * Both the simple sub-object form (\texttt{profile:*}) and the
   * \texttt{List<BaseMsg>} form (\texttt{addresses[*]:*}) and the filtered
   * variant (\texttt{sessions[type='guest']:*}) parse without syntax errors.
   * Semantic validation of "wildcard only as terminal" is checked separately
   * in {@link TestErasurePolicyValidator}.
   */
  @Test
  public void wildcardTerminalParses() {
    ParseResult result = parse(PolicyTestResources.load("valid-wildcard-terminal.erp"));
    assertTrue(
        "expected no syntax errors for wildcard terminal forms but got " + result.errors,
        result.errors.isEmpty());
  }

  /**
   * The grammar permits STAR at any pathStep position; the wildcard-only-as-
   * terminal property is enforced by the semantic validator, not the parser.
   * This test confirms the parser-permissive design: an interior STAR
   * ({@code profile:*:firstName}) is accepted by the parser so that the
   * validator can produce a focused, location-aware error message instead of
   * the generic syntax error a stricter grammar would emit.
   */
  @Test
  public void wildcardInteriorIsSyntacticallyAccepted() {
    ParseResult result = parse(PolicyTestResources.load("invalid-wildcard-interior.erp"));
    assertTrue(
        "expected interior STAR to parse (semantic check is in the validator) but got "
            + result.errors,
        result.errors.isEmpty());
  }

  @Test
  public void hashCommentsAreSkipped() {
    assertTrue(parse(PolicyTestResources.load("with-hash-comments.erp")).errors.isEmpty());
  }

  @Test
  public void missingIdentityFails() {
    assertEquals(
        "missing IDENTITY declaration should produce a syntax error",
        1, parse(PolicyTestResources.load("missing-identity.erp")).errors.size());
  }

  @Test
  public void unknownKeywordFails() {
    assertTrue(
        "unknown keyword 'OBLITERATE' should produce at least one syntax error",
        !parse(PolicyTestResources.load("unknown-keyword.erp")).errors.isEmpty());
  }

  /**
   * Tokens are case-sensitive at the lexer level: lowercase 'identity'
   * matches the ID rule, not the IDENTITY keyword, so parsing fails.
   */
  @Test
  public void keywordsAreCaseSensitive() {
    assertTrue(
        "lowercase keywords should fail to parse",
        !parse(PolicyTestResources.load("lowercase-keywords.erp")).errors.isEmpty());
  }

  /** The SQL-style '--' line comment is intentionally not supported by the
   *  DSL grammar (it is reserved for HiveQL); using it should fail to parse. */
  @Test
  public void dashDashIsNotAComment() {
    assertTrue(
        "'--' should no longer be recognised as a DSL comment",
        !parse(PolicyTestResources.load("dash-dash-comment.erp")).errors.isEmpty());
  }

  /** Block comments '/&#42; ... &#42;/' are also no longer supported. */
  @Test
  public void slashStarIsNotAComment() {
    assertTrue(
        "'/* ... */' should no longer be recognised as a DSL comment",
        !parse(PolicyTestResources.load("block-comment.erp")).errors.isEmpty());
  }

  // ---------------------------------------------------------------------------

  private static class ParseResult {
    final List<String> errors;
    ParseResult(List<String> errors) {
      this.errors = errors;
    }
  }

  private static ParseResult parse(String src) {
    ErasurePolicyLexer lexer = new ErasurePolicyLexer(CharStreams.fromString(src));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ErasurePolicyParser parser = new ErasurePolicyParser(tokens);

    List<String> errors = new ArrayList<>();
    BaseErrorListener listener = new BaseErrorListener() {
      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                              int line, int charPositionInLine,
                              String msg, RecognitionException e) {
        errors.add("line " + line + ":" + charPositionInLine + " " + msg);
      }
    };
    lexer.removeErrorListeners();
    lexer.addErrorListener(listener);
    parser.removeErrorListeners();
    parser.addErrorListener(listener);

    parser.policy();
    return new ParseResult(errors);
  }
}
