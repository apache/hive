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

import java.util.ArrayList;
import java.util.List;

public final class PolicyGenerator {

  private PolicyGenerator() {
  }

  public static String generate(int s, int r, int policyIx, double conflictDensity) {
    final int sharedRules = Math.max(0, Math.min(r, (int) Math.round(r * conflictDensity)));
    final boolean sharedToErase = (policyIx % 2) == 0;
    final StringBuilder sb = new StringBuilder(256 + s * r * 48);
    sb.append("VERSION v").append(policyIx).append('\n');
    sb.append("IDENTITY userId TYPE INT\n");
    sb.append("SCHEMA TYPE INT\n");
    for (int si = 0; si < s; si++) {
      final int schemaId = 1000 + si;
      sb.append("FOR SCHEMA ").append(schemaId).append('\n');

      final List<String> erasePaths = new ArrayList<>();
      final List<String> replacePaths = new ArrayList<>();
      for (int ri = 0; ri < r; ri++) {
        if (ri < sharedRules) {
          final String path = "shared:f" + ri;
          (sharedToErase ? erasePaths : replacePaths).add(path);
        } else {
          final String path = "p" + policyIx + ":f" + ri;
          ((ri & 1) == 0 ? erasePaths : replacePaths).add(path);
        }
      }

      if (!erasePaths.isEmpty()) {
        sb.append("    ERASE ");
        for (int i = 0; i < erasePaths.size(); i++) {
          if (i > 0) sb.append(", ");
          sb.append(erasePaths.get(i));
        }
        sb.append('\n');
      }
      if (!replacePaths.isEmpty()) {
        sb.append("    REPLACE ");
        for (int i = 0; i < replacePaths.size(); i++) {
          if (i > 0) sb.append(", ");
          sb.append(replacePaths.get(i)).append(" = 'v").append(policyIx).append('\'');
        }
        sb.append('\n');
      }
    }
    return sb.toString();
  }
}
