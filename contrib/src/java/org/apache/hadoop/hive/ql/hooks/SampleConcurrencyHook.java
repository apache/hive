/**
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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of a compile time hook to enable concurrency for a subset
 * of queries
 */
public class SampleConcurrencyHook extends AbstractSemanticAnalyzerHook {


  // Set concurrency for a sample of the queries
  @Override
  public ASTNode preAnalyze(
    HiveSemanticAnalyzerHookContext context,
    ASTNode ast) throws SemanticException {
    HiveSemanticAnalyzerHookContextImpl ctx = (HiveSemanticAnalyzerHookContextImpl)context;
    HiveConf conf = (HiveConf)ctx.getConf();

    // If concurrency is disabled, nothing to do
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (!supportConcurrency) {
      return ast;
    }

    // Do nothing is the statement is show locks
    if (ast.getToken().getType() == HiveParser.TOK_SHOWLOCKS) {
      return ast;
    }

    //
    // based on sample rate, decide whether to gather stats
    //
    float pubPercent = conf.getFloat(FBHiveConf.ENABLE_PARTIAL_CONCURRENCY, 0);

    try {
      if (!HookUtils.rollDice(pubPercent)) {
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
        return ast;
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    return ast;
  }

  // Nothing to do
  @Override
  public void postAnalyze(
    HiveSemanticAnalyzerHookContext context,
    List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    // no nothing
  }
}
