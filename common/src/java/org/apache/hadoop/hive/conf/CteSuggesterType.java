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
package org.apache.hadoop.hive.conf;

/**
 * Type of suggester used for common table expression (CTE) detection and materialization.
 */
public enum CteSuggesterType {
  /**
   * Materialization is based on the AST/SQL structure of the query. The suggester only works when the
   * query explicitly defines CTEs using WITH clauses. The suggester applies early during the syntactic analysis phase
   * of the query and materializes WITH clauses into tables using heuristics and configured thresholds.
   */
  AST,
  /**
   * Materialization is based on the algebraic structure of the query. The suggester applies during the cost-based
   * optimization phase and the exact behavior can be configured via
   * {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#HIVE_CTE_SUGGESTER_CLASS} property.
   */
  CBO,
  /**
   * Materialization is disabled.
   */
  NONE;

  public boolean enabled(HiveConf conf) {
    return this.name().equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVE_CTE_SUGGESTER_TYPE));
  }
}
