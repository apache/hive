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

package org.apache.hadoop.hive.ql.anon.hooks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.tez.common.counters.TezCounters;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_COUNTER_GROUP;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_CTR_FILES_REWRITTEN;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErasureRunCompletionHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(ErasureRunCompletionHook.class);

  @Override
  public void run(final HookContext hookContext) {
    if (hookContext == null) {
      return;
    }

    final SessionState ssExtract = SessionState.get();
    if (ssExtract != null) {
      final String outFile = ssExtract.getHiveVariables().remove("anon.extract.outFile");
      final String extractStaging = ssExtract.getHiveVariables().remove("anon.extract.stagingDir");
      if (outFile != null && extractStaging != null) {
        try {
          mergeExtractToJsonArray(ssExtract.getConf(), extractStaging, outFile);
        } catch (Exception e) {
          LOG.error("ErasureRunCompletionHook: failed to write EXTRACT output file '{}': {}",
              outFile, e.getMessage(), e);
        }
      }
    }

    final HiveOperation op = hookContext.getQueryPlan() == null
        ? null : hookContext.getQueryPlan().getOperation();
    if (op != HiveOperation.ANON_TABLE) {
      return;
    }

    final SessionState ss = SessionState.get();
    if (ss == null) {
      return;
    }
    final String tblIdStr = ss.getHiveVariables().get("anon.run.tblId");
    final String startedTsStr = ss.getHiveVariables().get("anon.run.startedTs");
    if (tblIdStr == null || startedTsStr == null) {
      return;
    }

    final long tblId;
    final long startedTs;
    try {
      tblId = Long.parseLong(tblIdStr);
      startedTs = Long.parseLong(startedTsStr);
    } catch (NumberFormatException nfe) {
      LOG.warn("ErasureRunCompletionHook: malformed (tblId, startedTs) stash: ({}, {})",
          tblIdStr, startedTsStr);
      return;
    }

    long runId = startedTs;
    final String runIdStr = ss.getHiveVariables().get("anon.run.runId");
    if (runIdStr != null) {
      try {
        runId = Long.parseLong(runIdStr);
      } catch (NumberFormatException ignored) {
      }
    }

    final ErasureRunStatus status = mapStatus(hookContext);
    final long completedTs = System.currentTimeMillis();
    final long matchesInspected = readCounter(ss, "anon.run.matchesInspected");
    final long matchesRedacted = readCounter(ss, "anon.run.matchesRedacted");
    final long matchesFlagged = readCounter(ss, "anon.run.matchesFlagged");
    final long filesRewritten = readFilesRewritten(hookContext);
    try {
      Hive.get().updateErasureRunCompletion(tblId, startedTs, completedTs, status,
          matchesInspected, matchesRedacted, matchesFlagged);
      LOG.info("ErasureRunCompletionHook: closed run tblId={} startedTs={} status={} completedTs={}"
              + " matches inspected={} redacted={} flagged={} filesRewritten={}",
          tblId, startedTs, status, completedTs,
          matchesInspected, matchesRedacted, matchesFlagged, filesRewritten);
      if (filesRewritten > 0L) {
        try {
          final ErasureRunAudit filesPatch = new ErasureRunAudit();
          filesPatch.setTblId(tblId);
          filesPatch.setStartedTs(startedTs);
          filesPatch.setFilesRewritten((int) Math.min(filesRewritten, Integer.MAX_VALUE));
          Hive.get().recordErasureRun(filesPatch);
        } catch (Exception e) {
          LOG.warn("ErasureRunCompletionHook: failed to stamp filesRewritten for tblId={} startedTs={}: {}",
              tblId, startedTs, e.getMessage());
        }
      }
      final boolean completed = Hive.get().completeErasureRunLock(tblId, runId);
      if (completed) {
        LOG.info("ErasureRunCompletionHook: released run-lock tblId={} runId={} status={}",
            tblId, runId, status);
      } else {
        LOG.warn("ErasureRunCompletionHook: run-lock not released for tblId={} runId={} "
            + "(no matching RUNNING lock -- already released or taken over by a newer run)",
            tblId, runId);
      }
    } catch (Exception e) {
      LOG.warn("ErasureRunCompletionHook: failed to close run tblId={} startedTs={}: {}",
          tblId, startedTs, e.getMessage());
    } finally {
      ss.getHiveVariables().remove("anon.run.tblId");
      ss.getHiveVariables().remove("anon.run.startedTs");
      ss.getHiveVariables().remove("anon.run.runId");
      ss.getHiveVariables().remove("anon.run.matchesInspected");
      ss.getHiveVariables().remove("anon.run.matchesRedacted");
      ss.getHiveVariables().remove("anon.run.matchesFlagged");
    }
  }

  static void mergeExtractToJsonArray(final Configuration conf,
                                      final String stagingDirStr,
                                      final String outFileStr) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final ArrayNode arr = mapper.createArrayNode();

    final Path stagingDir = new Path(stagingDirStr);
    final FileSystem stagingFs = stagingDir.getFileSystem(conf);
    if (stagingFs.exists(stagingDir)) {
      final FileStatus[] parts = stagingFs.listStatus(stagingDir,
          p -> p.getName().startsWith("part-") && p.getName().endsWith(".json"));
      for (final FileStatus part : parts) {
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(stagingFs.open(part.getPath()), StandardCharsets.UTF_8))) {
          String line;
          while ((line = r.readLine()) != null) {
            if (!line.isBlank()) {
              arr.add(mapper.readTree(line));
            }
          }
        }
      }
    }

    final Path outFile = new Path(outFileStr);
    final FileSystem outFs = outFile.getFileSystem(conf);
    if (outFile.getParent() != null) {
      outFs.mkdirs(outFile.getParent());
    }
    try (FSDataOutputStream os = outFs.create(outFile, true)) {
      os.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(arr));
    }
    LOG.info("ErasureRunCompletionHook: wrote EXTRACT projection ({} entries) to {}",
        arr.size(), outFile);
  }

  private static long readCounter(final SessionState ss, final String key) {
    final String v = ss.getHiveVariables().get(key);
    if (v == null || v.isEmpty()) {
      return 0L;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException nfe) {
      return 0L;
    }
  }

  private static long readFilesRewritten(final HookContext hookContext) {
    try {
      final QueryPlan plan = hookContext.getQueryPlan();
      if (plan == null) {
        return 0L;
      }
      long sum = 0L;
      for (final TezTask task : Utilities.getTezTasks(plan.getRootTasks())) {
        final TezCounters counters = task.getTezCounters();
        if (counters != null) {
          sum += counters.findCounter(ANON_COUNTER_GROUP, ANON_CTR_FILES_REWRITTEN).getValue();
        }
      }
      return sum;
    } catch (Throwable t) {
      LOG.warn("ErasureRunCompletionHook: could not read filesRewritten counter: {}", t.getMessage());
      return 0L;
    }
  }

  private static ErasureRunStatus mapStatus(final HookContext ctx) {
    final HookContext.HookType type = ctx.getHookType();
    if (type == HookContext.HookType.ON_FAILURE_HOOK) {
      return ErasureRunStatus.FAILED;
    }
    return ErasureRunStatus.SUCCEEDED;
  }
}
