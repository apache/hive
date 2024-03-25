/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionIdentitySuggester;

import java.util.regex.Pattern;

public class CommonTableExpressionAutoTuningHook implements QueryLifeTimeHook {
  private static final Pattern WITH_KEYWORD = Pattern.compile("\\bWITH\\b", Pattern.CASE_INSENSITIVE);

  @Override
  public void beforeCompile(final QueryLifeTimeHookContext ctx) {
    if (WITH_KEYWORD.matcher(ctx.getCommand()).find()) {
      return;
    }
    HiveConf conf = ctx.getHiveConf();
    conf.setIntVar(HiveConf.ConfVars.HIVE_CTE_MATERIALIZE_THRESHOLD, 1);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CTE_MATERIALIZE_FULL_AGGREGATE_ONLY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_CTE_SUGGESTER_CLASS,
        CommonTableExpressionIdentitySuggester.class.getCanonicalName());
  }

  @Override
  public void afterCompile(final QueryLifeTimeHookContext ctx, final boolean hasError) {

  }

  @Override
  public void beforeExecution(final QueryLifeTimeHookContext ctx) {

  }

  @Override
  public void afterExecution(final QueryLifeTimeHookContext ctx, final boolean hasError) {

  }
}
