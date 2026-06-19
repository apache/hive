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
import org.apache.hive.hep.ParsedPolicy.Rule;
import org.apache.hive.hep.ParsedPolicy.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Detects and (optionally) resolves the four classes of cross-policy rule
 * conflict described in Section 5.6 of the paper.
 * <p>
 * Conflict classes (C1)--(C4):
 * <ul>
 *   <li><b>C1 action</b>: same {@code (schema, path)} with different action kinds
 *       (one ERASE, one REPLACE).</li>
 *   <li><b>C2 value</b>: same {@code (schema, path)} both REPLACE but with
 *       different literal values.</li>
 *   <li><b>C3 path-prefix</b>: same schema, one path covers a region of the
 *       message that the other reaches. Two forms qualify: a wildcard-terminal
 *       path {@code X:*} covers any longer path {@code X:Y} that descends
 *       from {@code X}, and (for compatibility with pre-wildcard policies) a
 *       bare colon-delimited prefix {@code X} is treated as covering any
 *       extension {@code X:Y}. The covering rule subsumes the field
 *       referenced by the covered rule.</li>
 *   <li><b>C4 schema-scope</b>: precondition only -- rules with different
 *       schema identifiers commute trivially.</li>
 * </ul>
 * Resolution under {@code STRICTEST}: ERASE wins over REPLACE; two competing
 * REPLACEs escalate to ERASE; outer-rule subsumes inner-rule, escalating to
 * ERASE if any disagreement exists.
 */
public final class PolicyConflictDetector {

  /** Resolution policy applied to detected conflicts. */
  public enum ResolutionMode { EXPLICIT, STRICTEST }

  /** Conflict class. */
  public enum ConflictClass { C1_ACTION, C2_VALUE, C3_PATH_PREFIX }

  /** A single detected conflict between two attributed rules. */
  public static final class Conflict {
    public final ConflictClass kind;
    public final String schemaId;
    public final AttributedRule first;
    public final AttributedRule second;

    public Conflict(ConflictClass kind, String schemaId,
                    AttributedRule first, AttributedRule second) {
      this.kind = kind;
      this.schemaId = schemaId;
      this.first = first;
      this.second = second;
    }

    @Override
    public String toString() {
      return "(" + kind.name() + ") on ('" + schemaId + "', "
          + first.rule.fieldPath + (kindEquals() ? "" : " / " + second.rule.fieldPath)
          + "): " + first.policyName + "." + first.rule
          + " vs " + second.policyName + "." + second.rule;
    }

    private boolean kindEquals() {
      return Objects.equals(first.rule.fieldPath, second.rule.fieldPath);
    }
  }

  /** A rule tagged with the policy that contributed it. */
  public static final class AttributedRule {
    public final String policyName;
    public final Rule rule;

    public AttributedRule(String policyName, Rule rule) {
      this.policyName = policyName;
      this.rule = rule;
    }
  }

  /** One resolved rule plus an audit trail describing how it was produced. */
  public static final class ResolvedRule {
    public final String schemaId;
    public final Rule rule;
    public final List<String> contributingPolicies;
    public final String resolutionNote; // null if no resolution was applied

    public ResolvedRule(String schemaId, Rule rule,
                        List<String> contributingPolicies, String resolutionNote) {
      this.schemaId = schemaId;
      this.rule = rule;
      this.contributingPolicies = Collections.unmodifiableList(
          new ArrayList<>(contributingPolicies));
      this.resolutionNote = resolutionNote;
    }
  }

  /** The full output of a detect-and-resolve pass. */
  public static final class Report {
    public final List<Conflict> conflicts;
    public final List<ResolvedRule> resolvedRules; // populated under STRICTEST; empty if EXPLICIT and conflicts present

    public Report(List<Conflict> conflicts, List<ResolvedRule> resolvedRules) {
      this.conflicts = Collections.unmodifiableList(new ArrayList<>(conflicts));
      this.resolvedRules = Collections.unmodifiableList(new ArrayList<>(resolvedRules));
    }

    public boolean hasConflicts() {
      return !conflicts.isEmpty();
    }
  }

  private PolicyConflictDetector() {
  }

  /**
   * Run the §5.6 detect-and-resolve algorithm over the unioned rule sets.
   *
   * @param attached  policies attached to the binding, in declared order;
   *                  may be empty
   * @param mode      resolution mode -- determines whether STRICTEST escalation
   *                  is applied to detected conflicts
   * @return a {@link Report} containing detected conflicts and, under
   *         {@code STRICTEST}, the resolved rule set
   */
  public static Report analyse(Map<String, ParsedPolicy> attached, ResolutionMode mode) {
    // (1) Flatten attached policies into per-schema lists of attributed rules.
    Map<String, List<AttributedRule>> perSchema = new LinkedHashMap<>();
    for (Map.Entry<String, ParsedPolicy> e : attached.entrySet()) {
      String policyName = e.getKey();
      for (Statement s : e.getValue().getStatements()) {
        perSchema.computeIfAbsent(s.schemaId, k -> new ArrayList<>());
        for (Rule r : s.rules) {
          perSchema.get(s.schemaId).add(new AttributedRule(policyName, r));
        }
      }
    }

    // (2) Detect conflicts per schema (C4 is implicit: cross-schema pairs never compared).
    List<Conflict> conflicts = new ArrayList<>();
    for (Map.Entry<String, List<AttributedRule>> e : perSchema.entrySet()) {
      detectConflictsWithinSchema(e.getKey(), e.getValue(), conflicts);
    }

    // (3) Under EXPLICIT, stop here -- resolution is not attempted.
    if (mode == ResolutionMode.EXPLICIT) {
      return new Report(conflicts, Collections.emptyList());
    }

    // (4) Under STRICTEST, build the resolved rule set.
    List<ResolvedRule> resolved = new ArrayList<>();
    for (Map.Entry<String, List<AttributedRule>> e : perSchema.entrySet()) {
      resolved.addAll(resolveSchema(e.getKey(), e.getValue()));
    }
    return new Report(conflicts, resolved);
  }

  // ---------------------------------------------------------------------------
  // Detection
  // ---------------------------------------------------------------------------

  private static void detectConflictsWithinSchema(String schemaId,
                                                  List<AttributedRule> rules,
                                                  List<Conflict> out) {
    for (int i = 0; i < rules.size(); i++) {
      for (int j = i + 1; j < rules.size(); j++) {
        AttributedRule a = rules.get(i);
        AttributedRule b = rules.get(j);
        if (a.policyName.equals(b.policyName)) {
          // Within-policy conflicts are handled by the validator; ignore here.
          continue;
        }
        if (a.rule.fieldPath.equals(b.rule.fieldPath)) {
          // C1 or C2 depending on action kinds.
          if (a.rule.action == ActionKind.ERASE && b.rule.action == ActionKind.REPLACE) {
            out.add(new Conflict(ConflictClass.C1_ACTION, schemaId, a, b));
          } else if (a.rule.action == ActionKind.REPLACE && b.rule.action == ActionKind.ERASE) {
            out.add(new Conflict(ConflictClass.C1_ACTION, schemaId, a, b));
          } else if (a.rule.action == ActionKind.REPLACE && b.rule.action == ActionKind.REPLACE
                     && !Objects.equals(a.rule.replaceValue, b.rule.replaceValue)) {
            out.add(new Conflict(ConflictClass.C2_VALUE, schemaId, a, b));
          }
        } else if (isProperPrefix(a.rule.fieldPath, b.rule.fieldPath)
                   || isProperPrefix(b.rule.fieldPath, a.rule.fieldPath)) {
          out.add(new Conflict(ConflictClass.C3_PATH_PREFIX, schemaId, a, b));
        }
      }
    }
  }

  /**
   * True iff a rule with path {@code prefix} covers the field referenced by a
   * rule with path {@code full}. Two forms qualify:
   *
   * <ul>
   *   <li>{@code prefix} ends with {@code ":*"} (the §4.2 wildcard-terminal
   *       form): the wildcard sweeps every descendant of the base path, so
   *       {@code prefix} covers any {@code full} whose initial steps equal
   *       the base and which extends further. This is the post-wildcard
   *       C3 detection path.</li>
   *   <li>{@code prefix} is a strict colon- or bracket-delimited string
   *       prefix of {@code full} (the legacy form): retained for backward
   *       compatibility with policies authored before the {@code :*}
   *       wildcard was introduced, and as a defence in depth in case a
   *       sub-object terminal path reaches the detector despite the
   *       validator's anonymisability check.</li>
   * </ul>
   */
  private static boolean isProperPrefix(String prefix, String full) {
    // Wildcard-terminal form: "X:*" covers any "X:Y..." where Y is anything.
    if (prefix.endsWith(":*")) {
      String base = prefix.substring(0, prefix.length() - 2);
      if (base.isEmpty() || prefix.equals(full)) {
        return false;
      }
      if (!full.startsWith(base)) {
        return false;
      }
      if (full.length() <= base.length()) {
        return false;
      }
      char boundary = full.charAt(base.length());
      return boundary == ':' || boundary == '[';
    }

    // Legacy string-prefix form.
    if (prefix.length() >= full.length()) {
      return false;
    }
    if (!full.startsWith(prefix)) {
      return false;
    }
    char boundary = full.charAt(prefix.length());
    return boundary == ':' || boundary == '[';
  }

  // ---------------------------------------------------------------------------
  // Resolution (STRICTEST)
  // ---------------------------------------------------------------------------

  private static List<ResolvedRule> resolveSchema(String schemaId,
                                                  List<AttributedRule> rules) {
    // Group by exact field path, resolve C1/C2 within each group, then sweep
    // for C3 path-prefix containment and escalate outer rules to ERASE when
    // an inner disagreement exists.
    Map<String, List<AttributedRule>> byPath = new LinkedHashMap<>();
    for (AttributedRule r : rules) {
      byPath.computeIfAbsent(r.rule.fieldPath, k -> new ArrayList<>()).add(r);
    }

    Map<String, ResolvedRule> bucket = new LinkedHashMap<>();
    for (Map.Entry<String, List<AttributedRule>> e : byPath.entrySet()) {
      bucket.put(e.getKey(), reduceByPath(schemaId, e.getValue()));
    }

    // C3 sweep: find any (outer, inner) where outer is a proper prefix of inner.
    // If outer is REPLACE or the two disagree on action/value, escalate outer
    // to ERASE and drop inner.
    List<String> paths = new ArrayList<>(bucket.keySet());
    Collections.sort(paths); // sort so prefixes come before their extensions
    List<String> toRemove = new ArrayList<>();
    for (int i = 0; i < paths.size(); i++) {
      for (int j = i + 1; j < paths.size(); j++) {
        String outer = paths.get(i);
        String inner = paths.get(j);
        if (!isProperPrefix(outer, inner)) {
          continue;
        }
        ResolvedRule o = bucket.get(outer);
        if (o == null) {
          continue;
        }
        ResolvedRule replaced = escalateToErase(o,
            "C3: outer ERASE dominates inner rule at " + inner);
        bucket.put(outer, replaced);
        toRemove.add(inner);
      }
    }
    for (String p : toRemove) {
      bucket.remove(p);
    }
    return new ArrayList<>(bucket.values());
  }

  /**
   * Resolve a group of rules that all target the same {@code (schema, path)}.
   * Implements C1 (ERASE wins) and C2 (differing REPLACEs escalate to ERASE).
   */
  private static ResolvedRule reduceByPath(String schemaId, List<AttributedRule> group) {
    AttributedRule first = group.get(0);
    if (group.size() == 1) {
      return new ResolvedRule(schemaId, first.rule,
          Collections.singletonList(first.policyName), null);
    }
    boolean anyErase = false;
    boolean anyReplace = false;
    boolean anyFlag = false;
    boolean anyInspect = false;
    String replaceValue = null;
    boolean differingReplaces = false;
    List<String> contributing = new ArrayList<>();
    for (AttributedRule r : group) {
      contributing.add(r.policyName);
      switch (r.rule.action) {
        case ERASE:
          anyErase = true;
          break;
        case REPLACE:
          anyReplace = true;
          if (replaceValue == null) {
            replaceValue = r.rule.replaceValue;
          } else if (!Objects.equals(replaceValue, r.rule.replaceValue)) {
            differingReplaces = true;
          }
          break;
        case FLAG:
          anyFlag = true;
          break;
        case INSPECT:
          anyInspect = true;
          break;
        default:
          break;
      }
    }
    // §5 STRICTEST ordering: ERASE > REPLACE > FLAG > INSPECT. Any pair that
    // disagrees on action collapses to the more-protective rule, with a C1
    // note recording the dominated action.
    if (anyErase && (anyReplace || anyFlag || anyInspect)) {
      return new ResolvedRule(schemaId,
          Rule.erase(first.rule.fieldPath),
          contributing,
          "C1: ERASE dominates lower-strength action");
    }
    if (anyReplace && differingReplaces) {
      return new ResolvedRule(schemaId,
          Rule.erase(first.rule.fieldPath),
          contributing,
          "C2: differing REPLACEs escalated to ERASE");
    }
    if (anyReplace && (anyFlag || anyInspect)) {
      // REPLACE dominates FLAG/INSPECT: a value substitution is at least as
      // protective as a record-only action.
      return new ResolvedRule(schemaId,
          Rule.replace(first.rule.fieldPath, replaceValue,
              firstReplaceLiteralKind(group)),
          contributing,
          "C1: REPLACE dominates FLAG/INSPECT");
    }
    if (anyFlag && anyInspect) {
      return new ResolvedRule(schemaId,
          Rule.flag(first.rule.fieldPath),
          contributing,
          "C1: FLAG dominates INSPECT");
    }
    // All rules agree; first is representative.
    return new ResolvedRule(schemaId, first.rule, contributing, null);
  }

  private static ParsedPolicy.LiteralKind firstReplaceLiteralKind(List<AttributedRule> group) {
    for (AttributedRule r : group) {
      if (r.rule.action == ActionKind.REPLACE && r.rule.replaceValueKind != null) {
        return r.rule.replaceValueKind;
      }
    }
    return null;
  }

  private static ResolvedRule escalateToErase(ResolvedRule r, String note) {
    if (r.rule.action == ActionKind.ERASE) {
      // Already an ERASE; just record the C3 outcome.
      return new ResolvedRule(r.schemaId, r.rule, r.contributingPolicies, note);
    }
    return new ResolvedRule(r.schemaId,
        Rule.erase(r.rule.fieldPath),
        r.contributingPolicies,
        note);
  }
}
