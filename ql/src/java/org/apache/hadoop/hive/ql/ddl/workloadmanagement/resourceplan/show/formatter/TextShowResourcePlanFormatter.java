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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.show.formatter;

import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Formats SHOW RESOURCE PLAN(S) results to text format.
 */
class TextShowResourcePlanFormatter extends ShowResourcePlanFormatter {
  @Override
  public void showResourcePlans(DataOutputStream out, List<WMResourcePlan> resourcePlans) throws HiveException {
    try {
      for (WMResourcePlan plan : resourcePlans) {
        out.write(plan.getName().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.tabCode);
        out.write(plan.getStatus().name().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.tabCode);
        String queryParallelism = plan.isSetQueryParallelism() ? Integer.toString(plan.getQueryParallelism()) : "null";
        out.write(queryParallelism.getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.tabCode);
        String defaultPoolPath = plan.isSetDefaultPoolPath() ? plan.getDefaultPoolPath() : "null";
        out.write(defaultPoolPath.getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void showFullResourcePlan(DataOutputStream out, WMFullResourcePlan resourcePlan) throws HiveException {
    formatFullRP(new TextRPFormatter(out), resourcePlan);
  }

  /**
   * Class to print text records for resource plans in the following format.
   * 
   * <rp_name>[status=<STATUS>,parallelism=<parallelism>,defaultPool=<defaultPool>]
   *     <queue_name>[allocFraction=<fraction>,schedulingPolicy=<policy>,parallelism=<parallelism>]
   *       > <trigger_name>: if(<triggerExpression>){<actionExpression>}
   */
  private static class TextRPFormatter implements RPFormatter {
    private static final byte[] INDENT = "    ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDENT2 = " |  ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INDENT_BRANCH = " +  ".getBytes(StandardCharsets.UTF_8);

    private final DataOutputStream out;
    private int indentLevel = 0;

    TextRPFormatter(DataOutputStream out) {
      this.out = out;
    }

    @Override
    public void startRP(String rpName, Object ... keyValuePairs) throws IOException {
      out.write(rpName.getBytes(StandardCharsets.UTF_8));
      writeFields(keyValuePairs);
      out.write(Utilities.newLineCode);
    }

    @Override
    public void endRP() throws IOException {
    }

    @Override
    public void startPools() throws IOException {
    }

    @Override
    public void endPools() throws IOException {
    }

    @Override
    public void startPool(String poolName, Object ... keyValuePairs) throws IOException {
      ++indentLevel;
      writeIndent(true);
      out.write(poolName.getBytes(StandardCharsets.UTF_8));
      writeFields(keyValuePairs);
      out.write(Utilities.newLineCode);
    }

    @Override
    public void endPool() throws IOException {
      --indentLevel;
    }

    @Override
    public void startTriggers() throws IOException {
    }

    @Override
    public void startMappings() throws IOException {
    }

    @Override
    public void endTriggers() throws IOException {
    }

    @Override
    public void endMappings() throws IOException {
    }

    private void writeFields(Object ... keyValuePairs) throws IOException {
      if (keyValuePairs.length % 2 != 0) {
        throw new IllegalArgumentException("Expected pairs, got: " + keyValuePairs.length);
      }

      if (keyValuePairs.length < 2) {
        return;
      }

      out.write('[');
      out.write(getKeyValueText(0, keyValuePairs).getBytes(StandardCharsets.UTF_8));
      for (int i = 2; i < keyValuePairs.length; i += 2) {
        out.write(',');
        out.write(getKeyValueText(i, keyValuePairs).getBytes(StandardCharsets.UTF_8));
      }
      out.write(']');
    }

    private String getKeyValueText(int i, Object ... keyValuePairs) {
      return String.format("%s=%s", keyValuePairs[i], keyValuePairs[i + 1] == null ? "null" : keyValuePairs[i + 1]);
    }

    @Override
    public void formatTrigger(String triggerName, String actionExpression, String triggerExpression)
        throws IOException {
      writeIndent(false);
      String triggerText =
          String.format("trigger %s: if (%s) { %s }", triggerName, triggerExpression, actionExpression);
      out.write(triggerText.getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.newLineCode);
    }

    @Override
    public void formatMappingType(String type, List<String> names) throws IOException {
      writeIndent(false);
      out.write(("mapped for " + type.toLowerCase()).getBytes(StandardCharsets.UTF_8));
      if (!names.isEmpty()) {
        out.write("s: ".getBytes(StandardCharsets.UTF_8));
        int count = Math.min(5, names.size());
        for (int i = 0; i < count; ++i) {
          if (i != 0) {
            out.write(", ".getBytes(StandardCharsets.UTF_8));
          }
          out.write(names.get(i).getBytes(StandardCharsets.UTF_8));
        }
        int remaining = names.size() - count;
        if (remaining > 0) {
          out.write((" and " + remaining + " others").getBytes(StandardCharsets.UTF_8));
        }
      }
      out.write(Utilities.newLineCode);
    }

    private void writeIndent(boolean isPool) throws IOException {
      for (int i = 0; i < indentLevel - 1; ++i) {
        out.write(INDENT);
      }
      if (isPool) {
        out.write(INDENT_BRANCH);
      } else {
        out.write(INDENT);
        out.write(INDENT2);
      }
    }
  }
}
