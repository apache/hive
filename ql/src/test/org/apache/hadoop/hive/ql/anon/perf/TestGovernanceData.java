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

public final class TestGovernanceData {

  private static final boolean FULL =
      Boolean.parseBoolean(System.getProperty("gov.perf.full", "false"));

  private static final boolean MEDIUM =
      !FULL && Boolean.parseBoolean(System.getProperty("gov.perf.medium", "false"));

  public static final int[] statementsPerPolicy = FULL
      ? new int[]{1, 4, 16, 64}
      : MEDIUM ? new int[]{1, 4, 16}
      : new int[]{1, 4};

  public static final int[] rulesPerStatement = FULL
      ? new int[]{1, 4, 16, 64}
      : MEDIUM ? new int[]{1, 4, 16}
      : new int[]{1, 4};

  public static final int[] policiesPerBinding = FULL
      ? new int[]{1, 2, 4, 8}
      : MEDIUM ? new int[]{1, 2, 4}
      : new int[]{1, 2};

  public static final ResolutionMode[] resolutionModes = FULL || MEDIUM
      ? new ResolutionMode[]{ResolutionMode.EXPLICIT, ResolutionMode.STRICTEST}
      : new ResolutionMode[]{ResolutionMode.STRICTEST};

  public static final double[] conflictDensity = FULL
      ? new double[]{0.0, 0.25, 0.5, 1.0}
      : MEDIUM ? new double[]{0.0, 0.5, 1.0}
      : new double[]{0.0, 1.0};

  public static final int[] auditTrailSizes = FULL
      ? new int[]{1_000, 10_000}
      : MEDIUM ? new int[]{1_000, 10_000}
      : new int[]{1_000};

  public static final int repetitions = FULL ? 3 : MEDIUM ? 3 : 2;
}
