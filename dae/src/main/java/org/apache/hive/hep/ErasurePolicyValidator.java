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
import org.antlr.v4.runtime.Token;
import org.apache.hive.hep.ErasurePolicyParser.EraseClauseContext;
import org.apache.hive.hep.ErasurePolicyParser.FieldPathContext;
import org.apache.hive.hep.ErasurePolicyParser.FieldTypeContext;
import org.apache.hive.hep.ErasurePolicyParser.FlagClauseContext;
import org.apache.hive.hep.ErasurePolicyParser.InspectClauseContext;
import org.apache.hive.hep.ErasurePolicyParser.LiteralContext;
import org.apache.hive.hep.ErasurePolicyParser.PathStepContext;
import org.apache.hive.hep.ErasurePolicyParser.PolicyContext;
import org.apache.hive.hep.ErasurePolicyParser.ReplaceClauseContext;
import org.apache.hive.hep.ErasurePolicyParser.ReplaceRuleContext;
import org.apache.hive.hep.ErasurePolicyParser.StatementContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses and semantically validates an erasure policy DSL source.
 * <p>
 * Performs both syntactic and semantic checks. Syntactic errors are surfaced
 * via the ANTLR parser's error listener; semantic errors are produced by this
 * class.
 * <p>
 * The four semantic checks correspond to the validation requirements stated in
 * Section 5.2.2 of the accompanying paper, plus one additional check for
 * replace-rule literal types:
 * <ol>
 *   <li>Every {@code FOR SCHEMA} block must contain at least one of
 *       {@code ERASE} or {@code REPLACE}.</li>
 *   <li>No field path may appear in both the {@code ERASE} and {@code REPLACE}
 *       clauses of the same block.</li>
 *   <li>The literal used as the {@code schemaId} in every {@code FOR SCHEMA}
 *       clause must match the type declared by {@code SCHEMA TYPE}.</li>
 *   <li>The literal on the right-hand side of a {@code REPLACE} rule must be
 *       a recognised scalar literal.</li>
 * </ol>
 * Checks that depend on the message schema (path existence, predicate target
 * must be a repeated field, filter literal type matches filtered subfield)
 * are not performed here; they belong to a schema-aware validation pass at
 * binding time.
 */
public final class ErasurePolicyValidator {

  /** A single validation error with source location. */
  public static final class ValidationError {
    public final int line;
    public final int column;
    public final String message;

    public ValidationError(int line, int column, String message) {
      this.line = line;
      this.column = column;
      this.message = message;
    }

    @Override
    public String toString() {
      return "line " + line + ":" + column + " " + message;
    }
  }

  /**
   * Aggregated result of one validation run. Carries any errors detected
   * (syntactic or semantic) and, when validation succeeded, the parsed
   * representation of the policy.
   */
  public static final class ValidationResult {
    private final List<ValidationError> errors;
    private final ParsedPolicy policy; // null when isValid() is false

    public ValidationResult(List<ValidationError> errors) {
      this(errors, null);
    }

    public ValidationResult(List<ValidationError> errors, ParsedPolicy policy) {
      this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
      this.policy = policy;
    }

    public boolean isValid() {
      return errors.isEmpty();
    }

    public List<ValidationError> getErrors() {
      return errors;
    }

    /**
     * The parsed policy, when validation succeeded. Returns {@code null} when
     * {@link #isValid()} is {@code false}.
     */
    public ParsedPolicy getPolicy() {
      return policy;
    }
  }

  private ErasurePolicyValidator() {
  }

  /**
   * Parse and validate the given erasure-policy source.
   *
   * @param src the full text of an {@code .erp} file
   * @return a {@link ValidationResult} containing any syntactic or semantic
   *         errors found; empty errors list indicates a valid policy
   */
  public static ValidationResult validate(String src) {
    List<ValidationError> errors = new ArrayList<>();

    ErasurePolicyLexer lexer = new ErasurePolicyLexer(CharStreams.fromString(src));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ErasurePolicyParser parser = new ErasurePolicyParser(tokens);

    BaseErrorListener errListener = new BaseErrorListener() {
      @Override
      public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                              int line, int column,
                              String msg, RecognitionException e) {
        errors.add(new ValidationError(line, column, msg));
      }
    };
    lexer.removeErrorListeners();
    lexer.addErrorListener(errListener);
    parser.removeErrorListeners();
    parser.addErrorListener(errListener);

    PolicyContext tree = parser.policy();

    // If parsing failed, semantic checks would just produce noise; bail.
    if (!errors.isEmpty()) {
      return new ValidationResult(errors);
    }

    LiteralKind declaredSchemaKind = literalKindFor(tree.schemaTypeDecl().fieldType());

    for (StatementContext stmt : tree.statement()) {
      checkAtLeastOneClause(stmt, errors);
      checkNoOverlappingPaths(stmt, errors);
      checkSchemaIdLiteralType(stmt, declaredSchemaKind, errors);
      checkReplaceLiteralsAreScalar(stmt, errors);
      checkWildcardTerminalOnly(stmt, errors);
    }

    if (!errors.isEmpty()) {
      return new ValidationResult(errors);
    }
    return new ValidationResult(errors, buildParsedPolicy(tree));
  }

  /**
   * Parse and validate {@code src} and return the resulting {@link ParsedPolicy}.
   * Throws if validation produces any errors.
   */
  public static ParsedPolicy parse(String src) {
    ValidationResult r = validate(src);
    if (!r.isValid()) {
      throw new IllegalArgumentException("invalid erasure policy: " + r.getErrors());
    }
    return r.getPolicy();
  }

  /**
   * Leniently read the mandatory {@code VERSION} header label from {@code src}
   * without running semantic validation. {@code LOAD} uses this because a draft
   * is work in progress: its body may not yet validate, but the version label
   * is required and must be read so the draft can be persisted under its own
   * declared label (VALIDATE surfaces body errors later). ANTLR error recovery
   * still yields the leading header declarations even when the body is invalid.
   * Returns {@code null} when the file declares no {@code VERSION} header, in
   * which case the caller refuses the file (there is no auto-numbered fallback).
   */
  public static String extractVersionLabel(String src) {
    ErasurePolicyLexer lexer = new ErasurePolicyLexer(CharStreams.fromString(src));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ErasurePolicyParser parser = new ErasurePolicyParser(tokens);
    // Suppress error reporting: we only want the leading header, and an
    // as-yet-invalid body must not raise here.
    lexer.removeErrorListeners();
    parser.removeErrorListeners();
    PolicyContext tree = parser.policy();
    if (tree == null) {
      return null;
    }
    for (ErasurePolicyParser.HeaderDeclContext h : tree.headerDecl()) {
      if (h.versionDecl() != null) {
        return headerValueText(h.versionDecl().headerValue());
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Check 1: every FOR SCHEMA block has at least one ERASE or REPLACE clause.
  // ---------------------------------------------------------------------------

  private static void checkAtLeastOneClause(StatementContext stmt,
                                            List<ValidationError> errors) {
    if (stmt.eraseClause() == null && stmt.replaceClause() == null
        && stmt.inspectClause() == null && stmt.flagClause() == null) {
      Token tok = stmt.getStart();
      errors.add(new ValidationError(tok.getLine(), tok.getCharPositionInLine(),
          "FOR SCHEMA " + textOf(stmt.schemaId().literal())
              + " must declare at least one of ERASE, REPLACE, INSPECT, or FLAG"));
    }
  }

  // ---------------------------------------------------------------------------
  // Check 2: no field path appears in both ERASE and REPLACE of the same block.
  // ---------------------------------------------------------------------------

  private static void checkNoOverlappingPaths(StatementContext stmt,
                                              List<ValidationError> errors) {
    EraseClauseContext erase = stmt.eraseClause();
    ReplaceClauseContext replace = stmt.replaceClause();
    if (erase == null || replace == null) {
      return;
    }
    // Capture path text -> first-seen token in the ERASE clause so error
    // messages point at the conflicting occurrence in REPLACE.
    Map<String, FieldPathContext> erasePaths = new HashMap<>();
    for (FieldPathContext fp : erase.fieldPath()) {
      erasePaths.putIfAbsent(fp.getText(), fp);
    }
    for (ReplaceRuleContext rr : replace.replaceRule()) {
      FieldPathContext rfp = rr.fieldPath();
      if (erasePaths.containsKey(rfp.getText())) {
        Token tok = rfp.getStart();
        errors.add(new ValidationError(tok.getLine(), tok.getCharPositionInLine(),
            "field path '" + rfp.getText()
                + "' appears in both ERASE and REPLACE of FOR SCHEMA "
                + textOf(stmt.schemaId().literal())));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Check 3: schemaId literal type matches declared SCHEMA TYPE.
  // ---------------------------------------------------------------------------

  private static void checkSchemaIdLiteralType(StatementContext stmt,
                                               LiteralKind declared,
                                               List<ValidationError> errors) {
    LiteralContext lit = stmt.schemaId().literal();
    LiteralKind actual = literalKindFor(lit);
    if (actual != declared) {
      Token tok = lit.getStart();
      errors.add(new ValidationError(tok.getLine(), tok.getCharPositionInLine(),
          "FOR SCHEMA " + lit.getText() + " uses a " + actual.name()
              + " literal but SCHEMA TYPE is declared " + declared.name()));
    }
  }

  // ---------------------------------------------------------------------------
  // Check 4: every REPLACE rule's literal is one of the supported scalar kinds.
  // (The grammar guarantees this; the check exists to catch any future grammar
  //  extensions that add unsupported literal forms.)
  // ---------------------------------------------------------------------------

  private static void checkReplaceLiteralsAreScalar(StatementContext stmt,
                                                    List<ValidationError> errors) {
    if (stmt.replaceClause() == null) {
      return;
    }
    for (ReplaceRuleContext rr : stmt.replaceClause().replaceRule()) {
      if (literalKindFor(rr.literal()) == LiteralKind.UNKNOWN) {
        Token tok = rr.literal().getStart();
        errors.add(new ValidationError(tok.getLine(), tok.getCharPositionInLine(),
            "REPLACE literal " + rr.literal().getText()
                + " is not a recognised scalar (INT, LONG, STRING)"));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Check 5: the standalone '*' wildcard step appears only as the terminal
  // step of a field path. The grammar permits STAR anywhere a pathStep is
  // accepted; this semantic check enforces the §4.2 "wildcard only as
  // terminal" property and produces a focused error message at the offending
  // path step rather than letting the runtime crash on an interior STAR.
  // ---------------------------------------------------------------------------

  private static void checkWildcardTerminalOnly(StatementContext stmt,
                                                List<ValidationError> errors) {
    List<FieldPathContext> fps = new ArrayList<>();
    if (stmt.eraseClause() != null) {
      fps.addAll(stmt.eraseClause().fieldPath());
    }
    if (stmt.replaceClause() != null) {
      for (ReplaceRuleContext rr : stmt.replaceClause().replaceRule()) {
        fps.add(rr.fieldPath());
      }
    }
    if (stmt.inspectClause() != null) {
      fps.addAll(stmt.inspectClause().fieldPath());
    }
    if (stmt.flagClause() != null) {
      fps.addAll(stmt.flagClause().fieldPath());
    }
    for (FieldPathContext fp : fps) {
      List<PathStepContext> steps = fp.pathStep();
      for (int i = 0; i < steps.size(); i++) {
        PathStepContext ps = steps.get(i);
        boolean isStar = ps.STAR() != null && ps.ID() == null;
        if (isStar && i != steps.size() - 1) {
          Token tok = ps.getStart();
          errors.add(new ValidationError(tok.getLine(), tok.getCharPositionInLine(),
              "wildcard '*' step is permitted only as the final step of a path '"
                  + fp.getText() + "'"));
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private enum LiteralKind { INT, LONG, STRING, UNKNOWN }

  private static LiteralKind literalKindFor(LiteralContext lit) {
    if (lit.STRING_LITERAL() != null) return LiteralKind.STRING;
    if (lit.LONG_LITERAL() != null)   return LiteralKind.LONG;
    if (lit.INT_LITERAL() != null)    return LiteralKind.INT;
    return LiteralKind.UNKNOWN;
  }

  private static LiteralKind literalKindFor(FieldTypeContext ftc) {
    if (ftc.STRING() != null) return LiteralKind.STRING;
    if (ftc.LONG() != null)   return LiteralKind.LONG;
    if (ftc.INT() != null)    return LiteralKind.INT;
    return LiteralKind.UNKNOWN;
  }

  private static String textOf(LiteralContext lit) {
    return lit == null ? "<unknown>" : lit.getText();
  }

  // ---------------------------------------------------------------------------
  // ParsedPolicy construction (only called when validation has succeeded).
  // ---------------------------------------------------------------------------

  private static ParsedPolicy buildParsedPolicy(PolicyContext tree) {
    String idName  = tree.identityDecl().fieldName().getText();
    ParsedPolicy.LiteralKind idKind = toPublicKind(literalKindFor(tree.identityDecl().fieldType()));
    ParsedPolicy.LiteralKind schemaKind = toPublicKind(literalKindFor(tree.schemaTypeDecl().fieldType()));

    List<ParsedPolicy.Statement> statements = new ArrayList<>();
    for (StatementContext stmt : tree.statement()) {
      List<ParsedPolicy.Rule> rules = new ArrayList<>();
      if (stmt.eraseClause() != null) {
        for (FieldPathContext fp : stmt.eraseClause().fieldPath()) {
          rules.add(ParsedPolicy.Rule.erase(fp.getText()));
        }
      }
      if (stmt.replaceClause() != null) {
        for (ReplaceRuleContext rr : stmt.replaceClause().replaceRule()) {
          ParsedPolicy.LiteralKind lk = toPublicKind(literalKindFor(rr.literal()));
          rules.add(ParsedPolicy.Rule.replace(
              rr.fieldPath().getText(),
              unquoteIfStringLiteral(rr.literal().getText()),
              lk));
        }
      }
      if (stmt.inspectClause() != null) {
        for (FieldPathContext fp : stmt.inspectClause().fieldPath()) {
          rules.add(ParsedPolicy.Rule.inspect(fp.getText()));
        }
      }
      if (stmt.flagClause() != null) {
        for (FieldPathContext fp : stmt.flagClause().fieldPath()) {
          rules.add(ParsedPolicy.Rule.flag(fp.getText()));
        }
      }
      LiteralContext schemaLit = stmt.schemaId().literal();
      statements.add(new ParsedPolicy.Statement(
          unquoteIfStringLiteral(schemaLit.getText()),
          toPublicKind(literalKindFor(schemaLit)),
          rules));
    }

    // Optional LPL-style headers (version / purpose / legal basis). Recorded
    // verbatim; not interpreted as executable logic.
    String version = null;
    String purpose = null;
    String legalBasis = null;
    for (ErasurePolicyParser.HeaderDeclContext h : tree.headerDecl()) {
      if (h.versionDecl() != null) {
        version = headerValueText(h.versionDecl().headerValue());
      } else if (h.purposeDecl() != null) {
        purpose = unquoteIfStringLiteral(h.purposeDecl().STRING_LITERAL().getText());
      } else if (h.legalBasisDecl() != null) {
        legalBasis = headerValueText(h.legalBasisDecl().headerValue());
      }
    }
    return new ParsedPolicy(idName, idKind, schemaKind, statements, version, purpose, legalBasis);
  }

  private static String headerValueText(ErasurePolicyParser.HeaderValueContext hv) {
    return hv == null ? null : unquoteIfStringLiteral(hv.getText());
  }

  private static ParsedPolicy.LiteralKind toPublicKind(LiteralKind k) {
    switch (k) {
      case INT:    return ParsedPolicy.LiteralKind.INT;
      case LONG:   return ParsedPolicy.LiteralKind.LONG;
      case STRING: return ParsedPolicy.LiteralKind.STRING;
      default:
        throw new IllegalStateException("unknown literal kind cannot reach ParsedPolicy");
    }
  }

  private static String unquoteIfStringLiteral(String raw) {
    if (raw != null && raw.length() >= 2
        && raw.charAt(0) == '\'' && raw.charAt(raw.length() - 1) == '\'') {
      return raw.substring(1, raw.length() - 1);
    }
    return raw;
  }
}
