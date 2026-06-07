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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Parsed representation of an erasure policy DSL document.
 * Produced by {@link ErasurePolicyValidator#parse(String)}.
 * Consumers (the conflict detector, the analyzer, the metastore loader)
 * walk this structure instead of re-running the parser.
 */
public final class ParsedPolicy {

  /** Scalar literal kinds supported by the DSL. */
  public enum LiteralKind { INT, LONG, STRING }

  /** Action kinds supported by the DSL. {@code REPLACE} carries a literal. */
  public enum ActionKind { ERASE, REPLACE, INSPECT, FLAG }

  /** A single rule inside a {@code FOR SCHEMA} block. */
  public static final class Rule {
    public final String fieldPath;          // verbatim path text (e.g. "profile:firstName" or "xs[0]:y")
    public final ActionKind action;
    public final String replaceValue;       // populated only when action == REPLACE; otherwise null
    public final LiteralKind replaceValueKind; // populated only when action == REPLACE; otherwise null

    public Rule(String fieldPath, ActionKind action,
                String replaceValue, LiteralKind replaceValueKind) {
      this.fieldPath = Objects.requireNonNull(fieldPath);
      this.action = Objects.requireNonNull(action);
      this.replaceValue = replaceValue;
      this.replaceValueKind = replaceValueKind;
    }

    public static Rule erase(String fieldPath) {
      return new Rule(fieldPath, ActionKind.ERASE, null, null);
    }

    public static Rule replace(String fieldPath, String value, LiteralKind kind) {
      return new Rule(fieldPath, ActionKind.REPLACE, value, kind);
    }

    public static Rule inspect(String fieldPath) {
      return new Rule(fieldPath, ActionKind.INSPECT, null, null);
    }

    public static Rule flag(String fieldPath) {
      return new Rule(fieldPath, ActionKind.FLAG, null, null);
    }

    @Override
    public String toString() {
      switch (action) {
        case ERASE:   return "ERASE " + fieldPath;
        case REPLACE: return "REPLACE " + fieldPath + " = " + replaceValue;
        case INSPECT: return "INSPECT " + fieldPath;
        case FLAG:    return "FLAG " + fieldPath;
        default:      return action + " " + fieldPath;
      }
    }
  }

  /** A {@code FOR SCHEMA '<id>'} block with its rules. */
  public static final class Statement {
    public final String schemaId;             // verbatim text of the schemaId literal
    public final LiteralKind schemaIdKind;    // INT / LONG / STRING -- the literal type used
    public final List<Rule> rules;

    public Statement(String schemaId, LiteralKind schemaIdKind, List<Rule> rules) {
      this.schemaId = Objects.requireNonNull(schemaId);
      this.schemaIdKind = Objects.requireNonNull(schemaIdKind);
      this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
    }
  }

  // --- top-level declarations ------------------------------------------------

  private final String identityFieldName;
  private final LiteralKind identityFieldType;
  private final LiteralKind schemaType;
  private final List<Statement> statements;

  // Optional LPL-style metadata headers; null when the .erp file omits them.
  // {@code version}, when present, becomes the policy version label at LOAD time.
  private final String version;
  private final String purpose;
  private final String legalBasis;

  public ParsedPolicy(String identityFieldName,
                      LiteralKind identityFieldType,
                      LiteralKind schemaType,
                      List<Statement> statements) {
    this(identityFieldName, identityFieldType, schemaType, statements, null, null, null);
  }

  public ParsedPolicy(String identityFieldName,
                      LiteralKind identityFieldType,
                      LiteralKind schemaType,
                      List<Statement> statements,
                      String version,
                      String purpose,
                      String legalBasis) {
    this.identityFieldName = Objects.requireNonNull(identityFieldName);
    this.identityFieldType = Objects.requireNonNull(identityFieldType);
    this.schemaType = Objects.requireNonNull(schemaType);
    this.statements = Collections.unmodifiableList(new ArrayList<>(statements));
    this.version = version;
    this.purpose = purpose;
    this.legalBasis = legalBasis;
  }

  public String getIdentityFieldName() {
    return identityFieldName;
  }

  public LiteralKind getIdentityFieldType() {
    return identityFieldType;
  }

  public LiteralKind getSchemaType() {
    return schemaType;
  }

  public List<Statement> getStatements() {
    return statements;
  }

  /** The optional {@code VERSION} header, or null when the file omits it. */
  public String getVersion() {
    return version;
  }

  /** The optional {@code PURPOSE} header, or null when the file omits it. */
  public String getPurpose() {
    return purpose;
  }

  /** The optional {@code LEGAL BASIS} header, or null when the file omits it. */
  public String getLegalBasis() {
    return legalBasis;
  }
}
