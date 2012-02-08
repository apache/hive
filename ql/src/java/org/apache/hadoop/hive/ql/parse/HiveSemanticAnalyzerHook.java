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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Hook;

/**
 * HiveSemanticAnalyzerHook allows Hive to be extended with custom
 * logic for semantic analysis of QL statements.  This interface
 * and any Hive internals it exposes are currently
 * "limited private and evolving" (unless otherwise stated elsewhere)
 * and intended mainly for use by the Howl project.
 *
 *<p>
 *
 * Note that the lifetime of an instantiated hook object is scoped to
 * the analysis of a single statement; hook instances are never reused.
 */
public interface HiveSemanticAnalyzerHook extends Hook {
  /**
   * Invoked before Hive performs its own semantic analysis on
   * a statement.  The implementation may inspect the statement AST and
   * prevent its execution by throwing a SemanticException.
   * Optionally, it may also augment/rewrite the AST, but must produce
   * a form equivalent to one which could have
   * been returned directly from Hive's own parser.
   *
   * @param context context information for semantic analysis
   *
   * @param ast AST being analyzed and optionally rewritten
   *
   * @return replacement AST (typically the same as the original AST unless the
   * entire tree had to be replaced; must not be null)
   */
  public ASTNode preAnalyze(
    HiveSemanticAnalyzerHookContext context,
    ASTNode ast) throws SemanticException;

  /**
   * Invoked after Hive performs its own semantic analysis on a
   * statement (including optimization).
   * Hive calls postAnalyze on the same hook object
   * as preAnalyze, so the hook can maintain state across the calls.
   *
   * @param context context information for semantic analysis
   * @param rootTasks root tasks produced by semantic analysis;
   * the hook is free to modify this list or its contents
   */
  public void postAnalyze(
    HiveSemanticAnalyzerHookContext context,
    List<Task<? extends Serializable>> rootTasks) throws SemanticException;
}
