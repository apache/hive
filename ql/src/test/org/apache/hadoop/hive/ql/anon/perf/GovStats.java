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

public class GovStats {

  public int    paramId;
  public int    statementsPerPolicy;
  public int    rulesPerStatement;
  public int    policiesPerBinding;
  public String mode;
  public double conflictDensity;
  public String runner;
  public int    runNumber;
  public int    error;

  public long   startNs;
  public long   endNs;

  public int    c1Count;
  public int    c2Count;
  public int    c3Count;
  public long   rulesEmitted;
  public long   resolvedRules;
  public int    rowsInserted;
  public long   bytesGenerated;

  public GovStats() {
    startNs = System.nanoTime();
  }

  public void end() {
    endNs = System.nanoTime();
  }

  public long elapsedUs() {
    return (endNs - startNs) / 1_000L;
  }

  public static String csvHeader() {
    return "id,S,R,P,mode,conflict_density,runner,time_us,run_num,error,"
        + "c1_count,c2_count,c3_count,rules_emitted,resolved_rules,rows_inserted,bytes\n";
  }

  public String csvRow() {
    return String.format(
        "%d,%d,%d,%d,%s,%.2f,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
        paramId, statementsPerPolicy, rulesPerStatement, policiesPerBinding,
        mode, conflictDensity, runner, elapsedUs(), runNumber, error,
        c1Count, c2Count, c3Count, rulesEmitted, resolvedRules, rowsInserted,
        bytesGenerated);
  }
}
