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

package org.apache.hadoop.hive.ql.anon.perf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyLifecycleEvent;
import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.metastore.api.PolicyLifecycleEventType;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.hep.ErasurePolicyValidator;
import org.apache.hive.hep.ParsedPolicy;
import org.apache.hive.hep.PolicyConflictDetector.Conflict;
import org.apache.hive.hep.PolicyConflictDetector.ConflictClass;
import org.apache.hive.hep.PolicyConflictDetector.Report;
import org.apache.hive.hep.PolicyConflictDetector.ResolutionMode;
import org.apache.hive.hep.PolicyConflictDetector;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
public class TestGovernancePerf extends BaseTest {

  private final Map<String, String> generatedPolicies = new LinkedHashMap<>();
  private final Map<String, ParsedPolicy> parsedPolicies = new LinkedHashMap<>();

  private static final long AUDIT_TBL_ID = 9_000_001L;
  private boolean auditTrailSeeded = false;
  private long auditFromTs;
  private long auditUntilTs;

  public void gen(final TestGovernanceParams params) {
    for (int i = 0; i < params.policiesPerBinding; i++) {
      final String key = key(params.id, i);
      final String text = PolicyGenerator.generate(
          params.statementsPerPolicy,
          params.rulesPerStatement,
          i,
          params.conflictDensity);
      generatedPolicies.put(key, text);
      parsedPolicies.put(key, ErasurePolicyValidator.parse(text));
    }
  }

  public void run(final TestGovernanceParams params, final BufferedWriter writer)
      throws IOException {
    for (int rep = 0; rep < TestGovernanceData.repetitions; rep++) {
      writer.write(runV(params, rep).csvRow());
      writer.write(runD(params, rep).csvRow());
      writer.write(runA(params, rep).csvRow());
      writer.write(runL(params, rep).csvRow());
      writer.write(runU(params, rep).csvRow());
    }
    writer.flush();
  }


  private GovStats runV(final TestGovernanceParams params, final int rep) {
    final GovStats st = init(params, "V", rep);
    final String key = key(params.id, 0);
    final String text = generatedPolicies.get(key);
    st.bytesGenerated = text.length();
    try {
      final ParsedPolicy parsed = ErasurePolicyValidator.parse(text);
      long rules = 0;
      for (ParsedPolicy.Statement s : parsed.getStatements()) {
        rules += s.rules.size();
      }
      st.rulesEmitted = rules;
    } catch (Exception e) {
      st.error = 1;
      LOG.warn("runV failed", e);
    }
    st.end();
    return st;
  }


  private GovStats runD(final TestGovernanceParams params, final int rep) {
    final GovStats st = init(params, "D", rep);
    try {
      final Map<String, ParsedPolicy> attached = new LinkedHashMap<>();
      for (int i = 0; i < params.policiesPerBinding; i++) {
        attached.put("p" + i, parsedPolicies.get(key(params.id, i)));
      }
      final Report report = PolicyConflictDetector.analyse(attached, params.mode);
      countConflicts(report.conflicts, st);
      st.resolvedRules = report.resolvedRules.size();
    } catch (Exception e) {
      st.error = 1;
      LOG.warn("runD failed", e);
    }
    st.end();
    return st;
  }

  private static void countConflicts(final List<Conflict> conflicts, final GovStats st) {
    for (Conflict c : conflicts) {
      if (c.kind == ConflictClass.C1_ACTION)      st.c1Count++;
      else if (c.kind == ConflictClass.C2_VALUE)  st.c2Count++;
      else if (c.kind == ConflictClass.C3_PATH_PREFIX) st.c3Count++;
    }
  }


  private GovStats runA(final TestGovernanceParams params, final int rep) {
    final GovStats st = init(params, "A", rep);
    try {
      for (int i = 0; i < params.policiesPerBinding; i++) {
        final String policyName = govPolicyName(params.id, i, rep);
        final ErasurePolicy ep = new ErasurePolicy(policyName);
        try {
          hive.createErasurePolicy(ep, true);
          st.rowsInserted++;
        } catch (Exception inner) {
          LOG.debug("createErasurePolicy {} skipped: {}", policyName, inner.getMessage());
        }
      }
      final Map<String, ParsedPolicy> attached = new LinkedHashMap<>();
      for (int i = 0; i < params.policiesPerBinding; i++) {
        attached.put("p" + i, parsedPolicies.get(key(params.id, i)));
      }
      st.resolvedRules = PolicyConflictDetector.analyse(attached, params.mode)
          .resolvedRules.size();
    }catch (Exception e) {
      st.error = 1;
      LOG.warn("runA failed", e);
    }
    st.end();
    return st;
  }


  private GovStats runL(final TestGovernanceParams params, final int rep) {
    final GovStats st = init(params, "L", rep);
    final int eventsToWrite = params.policiesPerBinding * 4;
    try {
      for (int i = 0; i < eventsToWrite; i++) {
        final ErasurePolicyLifecycleEvent evt = new ErasurePolicyLifecycleEvent();
        evt.setVersionId(1L + i);
        evt.setEventType(PolicyLifecycleEventType.VALIDATED);
        evt.setPrincipal("perf");
        evt.setEventTs(System.currentTimeMillis());
        evt.setNote("L runner");
        try {
          hive.recordLifecycleEvent(evt);
          st.rowsInserted++;
        } catch (Exception inner) {
          LOG.debug("recordLifecycleEvent skipped: {}", inner.getMessage());
        }
      }
    }  catch (Exception e) {
      st.error = 1;
      LOG.warn("runL failed", e);
    }
    st.end();
    return st;
  }


  private void seedAuditTrailOnce() {
    if (auditTrailSeeded) {
      return;
    }
    auditTrailSeeded = true;
    final long now = System.currentTimeMillis();
    auditFromTs  = now - 86_400_000L;
    auditUntilTs = now + 86_400_000L;
    final int seedSize = TestGovernanceData.auditTrailSizes[
        TestGovernanceData.auditTrailSizes.length - 1];
    try {
      for (int i = 0; i < seedSize; i++) {
        final ErasureRunAudit run = new ErasureRunAudit();
        run.setTblId(AUDIT_TBL_ID);
        run.setColumnName("b");
        run.setBindingId(0L);
        run.setPrincipal("perf");
        run.setStartedTs(now - i);
        run.setStatus(ErasureRunStatus.SUCCEEDED);
        try {
          hive.recordErasureRun(run);
        } catch (Exception inner) {
          LOG.debug("recordErasureRun seed aborted at i={} : {}", i, inner.getMessage());
          return;
        }
      }
      LOG.info("seeded {} MErasureRunAudit rows for runner U", seedSize);
    } catch (Exception he) {
      LOG.info("seedAuditTrailOnce: no Hive client available ({})", he.getMessage());
    }
  }

  private GovStats runU(final TestGovernanceParams params, final int rep) {
    final GovStats st = init(params, "U", rep);
    try {
      seedAuditTrailOnce();
      st.startNs = System.nanoTime();
      final List<ErasureRunAudit> rows = hive.getErasureRunsForTable(
          AUDIT_TBL_ID, auditFromTs, auditUntilTs, null, null);
      st.rowsInserted = rows == null ? 0 : rows.size();
    } catch (HiveException he) {
      st.error = 1;
      LOG.info("runU: no Hive client available ({})", he.getMessage());
    } catch (Exception e) {
      st.error = 1;
      LOG.warn("runU failed", e);
    }
    st.end();
    return st;
  }


  private static GovStats init(final TestGovernanceParams p, final String runner, final int rep) {
    final GovStats st = new GovStats();
    st.paramId = p.id;
    st.statementsPerPolicy = p.statementsPerPolicy;
    st.rulesPerStatement = p.rulesPerStatement;
    st.policiesPerBinding = p.policiesPerBinding;
    st.mode = p.mode.name();
    st.conflictDensity = p.conflictDensity;
    st.runner = runner;
    st.runNumber = rep;
    return st;
  }

  private static String key(final int paramId, final int policyIx) {
    return paramId + "/" + policyIx;
  }

  private static String govPolicyName(final int paramId, final int policyIx, final int rep) {
    return "gov_p" + paramId + "_" + policyIx + "_" + rep;
  }


  @Test
  public void testGovPerf() {
    Assumptions.assumeTrue(Boolean.getBoolean("perf.run"), "set -Dperf.run=true to run this benchmark");
    runSweep();
  }

  void runSweep() {
    final List<TestGovernanceParams> lst = TestGovernanceParams.getLst();
    LOG.info("gov perf param-list size: {}", lst.size());
    final Path outDir = Paths.get("target");
    try {
      Files.createDirectories(outDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Path csvPath = outDir.resolve("gov-stats-" + System.currentTimeMillis() + ".csv");

    try (final BufferedWriter w = Files.newBufferedWriter(csvPath)) {
      w.write(GovStats.csvHeader());
      for (final TestGovernanceParams p : lst) {
        LOG.info("gov perf: {}", p);
        gen(p);
        run(p, w);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      LOG.info("gov perf done; output={}", csvPath);
    }
  }

  @Test
  public void testAuditTrailScale() throws Exception {
    Assumptions.assumeTrue(Boolean.getBoolean("perf.run"), "set -Dperf.run=true to run this benchmark");
    if (hive == null) {
      LOG.info("audit-scale: no Hive client; skipping");
      return;
    }
    final int[] sizes = {1_000, 10_000, 100_000};
    final int reps = 7;
    final long now = System.currentTimeMillis();
    final long from = now - 7L * 86_400_000L;
    final long until = now + 86_400_000L;
    final Path outDir = Paths.get("target");
    Files.createDirectories(outDir);
    final Path csv = outDir.resolve("gov-audit-scale-" + System.currentTimeMillis() + ".csv");
    try (final BufferedWriter w = Files.newBufferedWriter(csv)) {
      w.write("trail_size,rep,latency_ms,rows\n");
      for (int si = 0; si < sizes.length; si++) {
        final int size = sizes[si];
        final long tblId = AUDIT_TBL_ID + 100L + si;
        for (int i = 0; i < size; i++) {
          final ErasureRunAudit run = new ErasureRunAudit();
          run.setTblId(tblId);
          run.setColumnName("b");
          run.setBindingId(0L);
          run.setPrincipal("perf");
          run.setStartedTs(now - i);
          run.setStatus(ErasureRunStatus.SUCCEEDED);
          hive.recordErasureRun(run);
        }
        LOG.info("audit-scale: seeded {} rows (tblId={})", size, tblId);
        for (int rep = 0; rep < reps; rep++) {
          final long t0 = System.nanoTime();
          final List<ErasureRunAudit> rows =
              hive.getErasureRunsForTable(tblId, from, until, null, null);
          final double ms = (System.nanoTime() - t0) / 1_000_000.0;
          final int n = rows == null ? 0 : rows.size();
          final String msStr = String.format(Locale.ROOT, "%.3f", ms);
          w.write(size + "," + rep + "," + msStr + "," + n + "\n");
          LOG.info("audit-scale size={} rep={} {}ms rows={}", size, rep, msStr, n);
        }
        w.flush();
      }
    }
    System.out.println("AUDIT_SCALE_CSV=" + csv.toAbsolutePath());
  }

  public static void main(final String[] args) {
    new TestGovernancePerf().runSweep();
  }
}
