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

import org.apache.hive.hep.PolicyConflictDetector.ResolutionMode;

import java.util.ArrayList;
import java.util.List;

public final class TestGovernanceParams {

  public int    id;
  public int    statementsPerPolicy;
  public int    rulesPerStatement;
  public int    policiesPerBinding;
  public ResolutionMode mode;
  public double conflictDensity;

  private static final List<TestGovernanceParams> lst = new ArrayList<>();

  public static List<TestGovernanceParams> getLst() {
    return lst;
  }

  static {
    int id = 1;
    for (final int s : TestGovernanceData.statementsPerPolicy) {
      for (final int r : TestGovernanceData.rulesPerStatement) {
        for (final int p : TestGovernanceData.policiesPerBinding) {
          for (final ResolutionMode mode : TestGovernanceData.resolutionModes) {
            for (final double cd : TestGovernanceData.conflictDensity) {
              final TestGovernanceParams params = new TestGovernanceParams();
              params.id = id++;
              params.statementsPerPolicy = s;
              params.rulesPerStatement = r;
              params.policiesPerBinding = p;
              params.mode = mode;
              params.conflictDensity = cd;
              lst.add(params);
            }
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return String.format("gov-%d S=%d R=%d P=%d %s cd=%.2f",
        id, statementsPerPolicy, rulesPerStatement, policiesPerBinding,
        mode.name(), conflictDensity);
  }
}
